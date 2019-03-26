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
	"github.com/pollution-visualizer/api/models"
	"github.com/rs/cors"
)

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

func processCSV(name string, fileName string, dataList *models.DataList) {
	csvFile, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
	}
	defer csvFile.Close()
	fmt.Println("CSV read")

	reader := csv.NewReader(csvFile)
	reader.FieldsPerRecord = -1

	csvData, err := reader.ReadAll()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var data models.Data
	var datas []models.Data

	max := 0.0
	min := 59079741.0

	for _, each := range csvData {
		data, _ := strconv.ParseFloat(each[3], 64)
		if data > max {
			max = data
		}

		if data < min {
			min = data
		}
	}
	fmt.Println("Max and min determined")

	for _, each := range csvData {
		data.Country = each[0]
		data.Year, _ = strconv.Atoi(each[2])
		x, _ := strconv.ParseFloat(each[3], 64)
		data.Norm = ((x - min) / (max - min))
		data.Waste = x
		data.Latitude, data.Longitude = getLongitud(string(each[0]))
		datas = append(datas, data)
	}

	// Convert to JSON
	dataList.Name = name
	dataList.DataSet = datas
}

func main() {
	port := os.Getenv("PORT")

	fmt.Println(port)
	if port == "" {
		log.Fatal("$PORT must be set")
	}

	var dataList models.DataList

	fmt.Println("Initiating Waste csv analysis")
	processCSV("waste", "data/waste.csv", &dataList)
	fmt.Println("Waste csv analysis finished")
	fmt.Println("Initiating Water csv analysis")
	processCSV("water", "data/water.csv", &dataList)
	fmt.Println("Water csv analysis finished")

	jsonData, err := json.Marshal(dataList)
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
	handler := cors.Default().Handler(mxRouter)
	e := http.ListenAndServe(":"+port, handler)
	if e != nil {
		log.Fatal("error en el servidor : ", e)
		return
	}

}
