package main

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	pq "github.com/lib/pq"
)

const metarURL = "https://www.aviationweather.gov/adds/dataserver_current/current/metars.cache.csv.gz"

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

var metarHeaders = []*regexp.Regexp{
	regexp.MustCompile("^No errors$"),
	regexp.MustCompile("^No warnings$"),
	regexp.MustCompile("^[0-9]* ms$"),
	regexp.MustCompile("^data source=metars$"),
	regexp.MustCompile("^[0-9]* results$"),
	regexp.MustCompile("raw_text,station_id,observation_time,latitude,longitude,temp_c,dewpoint_c,wind_dir_degrees,wind_speed_kt,wind_gust_kt,visibility_statute_mi,altim_in_hg,sea_level_pressure_mb,corrected,auto,auto_station,maintenance_indicator_on,no_signal,lightning_sensor_off,freezing_rain_sensor_off,present_weather_sensor_off,wx_string,sky_cover,cloud_base_ft_agl,sky_cover,cloud_base_ft_agl,sky_cover,cloud_base_ft_agl,sky_cover,cloud_base_ft_agl,flight_category,three_hr_pressure_tendency_mb,maxT_c,minT_c,maxT24hr_c,minT24hr_c,precip_in,pcp3hr_in,pcp6hr_in,pcp24hr_in,snow_in,vert_vis_ft,metar_type,elevation_m"),
}

type Flags struct {
	dbURL    string
	filename string
	download bool
}

func (f *Flags) Parse(args []string) {
	fs := flag.NewFlagSet("", flag.ExitOnError)
	fs.StringVar(&f.dbURL, "dburl", "", "url or connection string to the database")
	fs.StringVar(&f.filename, "filename", "", "filename to read from")
	fs.BoolVar(&f.download, "download", true, "if set, file will be downloaded and deleted on success")
	fs.Parse(args)
}

func main() {
	flags := &Flags{}
	flags.Parse(os.Args[1:])
	if err := run(flags); err != nil {
		log.Fatal(err)
	}
}

func run(flags *Flags) error {
	if flags.download {
		if err := downloadFile(metarURL, flags.filename); err != nil {
			return fmt.Errorf("downloading file: %w", err)
		}
	}

	db, err := sql.Open("postgres", flags.dbURL)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}

	if err := fileToDB(db, flags.filename); err != nil {
		return fmt.Errorf("storing in database: %w", err)
	}
	if flags.download {
		if err := os.Remove(flags.filename); err != nil {
			return fmt.Errorf("removing file: %w", err)
		}
	}
	return nil
}

func fileToDB(db *sql.DB, fname string) error {
	file, err := os.Open(fname)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	scanner := bufio.NewScanner(file)
	if err := checkLines(metarHeaders, scanner); err != nil {
		return fmt.Errorf("bad headers: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()
	for scanner.Scan() {
		text := scanner.Text()
		if err := writeLine(tx, text); err != nil {
			return fmt.Errorf("writing line %q: %w", text, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading file: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing: %w", err)
	}
	return nil
}

func checkLines(patterns []*regexp.Regexp, scanner *bufio.Scanner) error {
	for _, pattern := range patterns {
		if !scanner.Scan() {
			return fmt.Errorf("scan error while looking for %v: %w", pattern, scanner.Err())
		}
		if text := scanner.Text(); !pattern.MatchString(text) {
			return fmt.Errorf("expected %v, got %q", pattern, text)
		}
	}
	return nil
}

func writeLine(tx *sql.Tx, text string) error {
	parts, err := csv.NewReader(strings.NewReader(text)).Read()
	if err != nil {
		return fmt.Errorf("parsing line: %w", err)
	}
	station := parts[1]
	observationTime, err := time.Parse(time.RFC3339, parts[2])
	if err != nil {
		return fmt.Errorf("bad time %q: %w", parts[2], err)
	}
	_, err = psql.Insert("metars").SetMap(map[string]interface{}{
		"station":   station,
		"time":      observationTime,
		"csv_parts": pq.StringArray(parts),
	}).
		Suffix("ON CONFLICT (station, time) DO UPDATE set csv_parts=EXCLUDED.csv_parts").
		RunWith(tx).
		Exec()
	if err != nil {
		return err
	}
	return nil
}

func downloadFile(url, filename string) error {
	resp, err := http.Get(metarURL)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}
	reader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("gzip error: %w", err)
	}
	outFile, err := os.OpenFile(filename, os.O_RDWR|os.O_EXCL|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("error creating file %q: %w", filename, err)
	}
	if _, err := io.Copy(outFile, reader); err != nil {
		return fmt.Errorf("error writing to file: %w", err)
	}
	return nil
}
