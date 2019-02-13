package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/gorilla/mux"
)

type Data struct {
	Country   string
	Year      int
	Waste     int
	Latitude  string
	Longitude string
}

func getLongitud(country string) (string, string) {
	var latitude string
	var longitude string
	doc, err := goquery.NewDocument("https://lab.lmnixon.org/4th/worldcapitals.html")
	if err != nil {
		log.Fatal(err)
	}
	doc.Find("table tr").Each(func(_ int, tr *goquery.Selection) {
		//var pais
		copy := 0
		tr.Find("td").Each(func(ix int, td *goquery.Selection) {
			if strings.ToUpper(td.Text()) == strings.ToUpper(country) {
				copy = 1
			}
			if copy == 1 && ix == 2 {
				temp := td.Text()
				if (temp[len(temp)-1:]) == "S" {
					latitude = "-" + temp[:len(temp)-1]
				} else {
					latitude = temp[:len(temp)-1]
				}
			}
			if copy == 1 && ix == 3 {
				temp := td.Text()
				if (temp[len(temp)-1:]) == "W" {
					longitude = "-" + temp[:len(temp)-1]
				} else {
					longitude = temp[:len(temp)-1]
				}
			}

		})
	})

	return latitude, longitude

}

func getData(w http.ResponseWriter, r *http.Request) {
	toReturn, _ := ioutil.ReadFile("data.json")

	w.Header().Set("Content-Type", "application/json")
	w.Write(toReturn)
	return
}

func main() {
	csvFile, err := os.Open("data/waste.csv")
	if err != nil {
		fmt.Println(err)
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	reader.FieldsPerRecord = -1

	csvData, err := reader.ReadAll()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var data Data
	var datas []Data

	for _, each := range csvData {
		data.Country = each[0]
		data.Year, _ = strconv.Atoi(each[2])
		data.Waste, _ = strconv.Atoi(each[3])
		data.Latitude, data.Longitude = getLongitud(string(each[0]))
		datas = append(datas, data)
	}

	// Convert to JSON
	jsonData, err := json.Marshal(datas)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	jsonFile, err := os.Create("./data.json")
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	jsonFile.Write(jsonData)
	jsonFile.Close()

	mxRouter := mux.NewRouter()
	mxRouter.HandleFunc("/", getData).Methods("GET")
	http.Handle("/", mxRouter)
	e := http.ListenAndServe("localhost"+":"+"3000", nil)
	if e != nil {
		log.Fatal("error en el servidor : ", e)
		return
	}

}
