package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/gorilla/mux"
	"github.com/pollution-visualizer/api/models"
	"github.com/rs/cors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var client *mongo.Client

func getLongitud(country string, doc *goquery.Document) (string, string) {
	var latitude string
	var longitude string
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

func getAllDocsFromCollections(name string) models.DataList {
	collection := client.Database("pollution-visualizer").Collection(name)

	cur, err := collection.Find(context.Background(), bson.D{{}})

	if err != nil {
		log.Fatal(err)
	}
	var dataList models.DataList
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		elem := &bson.D{}
		if err = cur.Decode(elem); err != nil {
			log.Fatal(err)
		}
		m := elem.Map()
		data := models.Data{
			Country:   m["country"].(string),
			Year:      m["year"].(int32),
			Waste:     m["waste"].(float64),
			Norm:      m["norm"].(float64),
			Latitude:  m["latitude"].(string),
			Longitude: m["longitude"].(string),
		}
		dataList.DataSet = append(dataList.DataSet, data)
	}
	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}

	dataList.Name = name
	return dataList
}

func getData(w http.ResponseWriter, r *http.Request) {
	//toReturn, _ := ioutil.ReadFile("data.json")

	waste := getAllDocsFromCollections("waste")
	water := getAllDocsFromCollections("water")

	var dataList []models.DataList

	dataList = append(dataList, waste)

	dataList = append(dataList, water)

	jsonData, err := json.Marshal(dataList)
	if err != nil {
		log.Fatal(err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
	return
}

func processCSV(name string, fileName string, doc *goquery.Document, wg *sync.WaitGroup) {
	csvFile, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer csvFile.Close()
	fmt.Println("CSV read")

	reader := csv.NewReader(csvFile)
	reader.FieldsPerRecord = -1

	csvData, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	var datas []interface{}
	var mutex = &sync.Mutex{}
	var numberOfGoroutines = len(csvData)
	var dataPerGoroutine = len(csvData) / numberOfGoroutines

	max := 0.0
	min := 59079741.0

	for i := 0; i < numberOfGoroutines; i++ {
		wg.Add(1)
		go func(i int, dataPerGoroutine int) {
			defer wg.Done()
			for j := (dataPerGoroutine * (i)); j < ((dataPerGoroutine * (i)) + dataPerGoroutine); j++ {
				data, _ := strconv.ParseFloat(csvData[j][3], 64)
				if data > max {
					max = data
				}

				if data < min {
					min = data
				}
			}
		}(i, dataPerGoroutine)
	}

	wg.Wait()

	for i := 0; i < numberOfGoroutines; i++ {
		wg.Add(1)
		go func(i int, dataPerGoroutine int) {
			defer wg.Done()
			for j := (dataPerGoroutine * (i)); j < ((dataPerGoroutine * (i)) + dataPerGoroutine); j++ {
				var data models.Data
				data.Country = csvData[j][0]
				temp, err := strconv.ParseInt(csvData[j][2], 10, 32)
				if err != nil {
					panic(err)
				}
				year := int32(temp)
				data.Year = year
				x, _ := strconv.ParseFloat(csvData[j][3], 64)
				data.Norm = ((x - min) / (max - min))
				data.Waste = x
				data.Latitude, data.Longitude = getLongitud(string(csvData[j][0]), doc)
				mutex.Lock()
				datas = append(datas, data)
				mutex.Unlock()
			}
		}(i, dataPerGoroutine)
	}

	wg.Wait()
	collection := client.Database("pollution-visualizer").Collection(name)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = collection.InsertMany(ctx, datas)
	if err != nil {
		log.Fatal(err)
	}
	return
}

func main() {
	port := os.Getenv("PORT")

	fmt.Println(port)
	if port == "" {
		log.Fatal("$PORT must be set")
	}

	doc, err := goquery.NewDocument("https://lab.lmnixon.org/4th/worldcapitals.html")
	if err != nil {
		log.Fatal(err)
	}
	user := os.Getenv("DATABASE_USER")
	password := os.Getenv("DATABASE_PASSWORD")
	connectionString := fmt.Sprintf("mongodb://%s:%s@ds117816.mlab.com:17816/pollution-visualizer", user, password)
	client, err = mongo.Connect(context.TODO(), options.Client().ApplyURI(connectionString))

	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB!")

	var wg sync.WaitGroup

	fmt.Println("Initiating Waste csv analysis")
	processCSV("waste", "data/waste.csv", doc, &wg)
	fmt.Println("Waste csv analysis finished")
	fmt.Println("Initiating Water csv analysis")
	processCSV("water", "data/water.csv", doc, &wg)
	fmt.Println("Water csv analysis finished")

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
