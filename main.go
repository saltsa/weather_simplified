package main

import (
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"runtime/pprof"
	"sort"

	"flag"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	_ "net/http/pprof"
)

const (
	dataFile    = "kaisaniemi.xml"
	opendataURL = "https://opendata.fmi.fi/wfs/fin?service=WFS&version=2.0.0"
)

type fc struct {
	XMLName xml.Name `xml:"http://www.opengis.net/wfs/2.0 FeatureCollection"`

	TS      string `xml:"timeStamp,attr"`
	Members []bwe  `xml:"member>BsWfsElement"`
}

type bwe struct {
	XMLName        xml.Name `xml:"http://xml.fmi.fi/schema/wfs/2.0 BsWfsElement"`
	Time           string
	ParameterName  string
	ParameterValue string
}

type d struct {
	Max float64
	Min float64
	Avg float64
}

func (d *d) Helle() bool {
	if d.Max > 25.0 {
		return true
	}
	return false
}

type weatherData struct {
	FMISID string
	Year   string
	Dates  map[string]d
}

func check(err error) {
	if err != nil {
		log.Errorf("Got fatal error: %v", err)
		time.Sleep(2 * time.Second)
		panic(err)
	}
}

func fetchDataHTTP(q url.Values) ([]byte, error) {

	u, err := url.Parse(opendataURL)
	check(err)

	u.RawQuery = q.Encode()
	url := u.String()

	client := http.Client{
		Timeout: 3 * time.Second,
	}

	log.Infof("Fetching data for FMI station %s with timeout %s", q.Get("fmisid"), client.Timeout)
	resp, err := client.Get(url)
	if err != nil {
		log.Errorf("http error: %s", err)
		return nil, err
	}

	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)

	return data, err
}

func fetchData(year, fmisid string, c chan *[]byte) {

	var data []byte
	var err error
	u := url.URL{}
	q := u.Query()
	q.Set("request", "GetFeature")
	q.Set("storedquery_id", "fmi::observations::weather::daily::simple")
	//q.Set("starttime", time.Now().AddDate(0, -2, 0).Format("2006-01-02"))
	q.Set("starttime", year+"-01-01")
	q.Set("endtime", year+"-12-31")
	q.Set("fmisid", fmisid) // https://ilmatieteenlaitos.fi/havaintoasemat

	data, err = fetchDataHTTP(q)
	if err != nil {
		log.Errorf("failed to fetch http data: %s", err)
		return
	}

	log.Info("File fetched and saved ok")

	c <- &data
}

func writeXMLToFile(data *[]byte) {
	dataFile := "failed.xml"
	tmpPath := "._new_" + dataFile
	f, err := os.OpenFile(tmpPath, os.O_EXCL|os.O_RDWR|os.O_CREATE, 0600) // fails if file exists
	check(err)
	_, err = f.Write(*data)
	check(err)
	err = f.Sync()
	check(err)
	err = f.Close()
	check(err)
	err = os.Rename(tmpPath, dataFile)
	check(err)
	log.Errorf("failed xml wrote to %s", dataFile)
}

func readData(data *[]byte) map[string]d {
	log.Debugf("Data received, parsing it")
	fcs := &fc{}

	umlStart := time.Now()
	err := xml.Unmarshal(*data, &fcs)
	log.Debugf("xml unmarshal took %v", time.Since(umlStart))
	if err != nil {
		log.Printf("XML error: %s", err)
		writeXMLToFile(data)
		panic("xml fail")
	}

	dates := make(map[string]d)

	for _, b := range fcs.Members {

		t, err := strconv.ParseFloat(b.ParameterValue, 64)
		if err != nil {
			log.Errorf("failed to parse value: %s", b.ParameterValue)
			continue
		}

		if math.IsNaN(t) {
			// skip nans
			continue
		}

		ti, err := time.Parse(time.RFC3339, b.Time)
		if err != nil {
			log.Errorf("failed to date: %s", b.Time)
			continue
		}
		localDate := ti.Local().Format("2006-01-02")
		tmp, ok := dates[localDate]
		if !ok {
			// new entry, use NaN as values instead of 0 as they're temps
			tmp.Min = math.NaN()
			tmp.Max = math.NaN()
			tmp.Avg = math.NaN()
		}

		switch b.ParameterName {
		case "tmax":
			tmp.Max = t
		case "tday":
			tmp.Avg = t
		case "tmin":
			tmp.Min = t
		}

		//log.Debugf("%s ty is %s, and for it's %+v", localDate, ty, tmp)

		dates[localDate] = tmp
	}

	return dates
}

func printDates(dates *map[string]d, target io.Writer) {
	names := make([]string, 0, len(*dates))
	for k := range *dates {
		names = append(names, k)
	}

	sort.Strings(names)

	var helleCount int64
	for _, k := range names {
		v := (*dates)[k]
		helle := ""
		if v.Helle() {
			helle = "hellepäivä"
			helleCount++
		}
		fmt.Fprintf(target, "%-16s min=%-7.2f avg=%-7.2f max=%-7.2f %16s\n", k, v.Min, v.Avg, v.Max, helle)
	}
	fmt.Fprintf(target, "\nTotal number of hellepäivät: %d\n", helleCount)
	//log.Printf("read xml: %+v", fcs)
}

func init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "15:04:05.000",
	})
	log.SetLevel(log.DebugLevel)

	// stdout is buffered hopefully
	log.SetOutput(os.Stdout)

}

func RequestLogger(targetMux http.Handler) http.Handler {
	log.Printf("Request logger called")
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		targetMux.ServeHTTP(w, r)

		// log request by who(IP address)
		requesterIP := r.RemoteAddr

		reqDuration := time.Since(start)

		vars := mux.Vars(r)
		log.Printf("%s %s\t%v\t%s", r.Method, requesterIP, reqDuration, vars["id"])
	})

	return handler
}

func main() {
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

	defaultFmiSid := flag.String("sid", "100971", "FMI station ID, defaults to Kaisaniemi, see https://ilmatieteenlaitos.fi/havaintoasemat")
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	log.Printf("cmdline fmisid: %s", *defaultFmiSid)

	ReturnWeatherData := func(w http.ResponseWriter, r *http.Request) {
		dataChan := make(chan *[]byte)
		vars := mux.Vars(r)

		log.Printf("Mux vars: %+v", vars)
		fmisid := *defaultFmiSid
		if vars["id"] != "" {
			fmisid = vars["id"]
		}

		year := r.FormValue("year")
		log.Debugf("get for year: %s", year)
		if len(year) != 4 {
			year = "2019"
		}

		log.Printf("get with fmisid %s", fmisid)
		go fetchData(year, fmisid, dataChan)

		select {
		case xml := <-dataChan:
			dates := readData(xml)

			d := &weatherData{}
			d.Dates = dates
			d.Year = year

			d.FMISID = fmisid

			fmt.Fprintf(w, "Data at %v year %s:\n\n", time.Now().Format("15:04:05"), year)
			printDates(&dates, w)
		case <-time.After(5 * time.Second):
			log.Errorf("channel read timeout")
			http.Error(w, "data read timeout", http.StatusInternalServerError)
		}
	}

	Usage := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/plain")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "query server with: /weather/<fmi station id>\n")
		fmt.Fprintf(w, "add optional ?year=NNNN for specific year")
	}

	mux := mux.NewRouter()
	mux.HandleFunc("/", Usage)
	mux.HandleFunc("/weather/{id:[0-9]*}", ReturnWeatherData)
	http.Handle("/", mux)

	// Allow http2 insecure
	h2s := &http2.Server{}
	myhandler := h2c.NewHandler(http.DefaultServeMux, h2s)

	s := &http.Server{
		Addr:         "localhost:8080",
		Handler:      myhandler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Fatal(s.ListenAndServe())

}
