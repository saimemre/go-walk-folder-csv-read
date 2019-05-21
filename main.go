package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"gopkg.in/mgo.v2"
	_ "gopkg.in/mgo.v2/bson"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Data struct {
	Name string `bson:"name"`
	Date time.Time `bson:"sale_date"`
	Open float32
	High float32
	Low float32
	Close float32
	Lot float32
	Volume float32
}

func main() {
	var (
    		files []string
    		err error
	  )

	session, err := mgo.Dial("localhost:27017")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	c := session.DB("saim").C("test2")

	root := "/Users/saim/Documents/Projects/akillifon-go-data-to-mysql"
	if len(os.Args) != 1 {
		log.Fatal("No path given, Please specify path.")
		return
	}
	// filepath.Walk
	 files, err = FilePathWalkDir(root)
	 if err != nil {
	 	panic(err)
	 }

	for _, file := range files {
		fileName := filepath.Base(file)
		baseName := strings.TrimSuffix(fileName, filepath.Ext(fileName))

		f, _ := os.Open(file)
		var values = ReadCsv(f)
		fmt.Println(values)

		for val := range values {

			s := strings.Split(values[val], ";")


			date, open, high, low, close := s[0], s[1], s[2], s[3], s[4]

			var lotFloat = float32(0)
			var volumeFloat = float32(0)

			if len(s) > 5 {
				lot, volume := s[5], s[6]

				lot64, _ := strconv.ParseFloat(lot, 64)
				lotFloat = float32(lot64)

				volume64, _ := strconv.ParseFloat(volume, 64)
				volumeFloat = float32(volume64)

			}


			dateParse := date[0:4] + "-" + date[4:6]+ "-" + date[6:8]

			layout := "2006-01-02"
			dateFormat, err := time.Parse(layout, dateParse)
			if err != nil {
				panic(err)
			}

			//fmt.Println(dateFormat, dateParse)

			open64, _ := strconv.ParseFloat(open, 64)
			var openFloat = float32(open64)

			high64, _ := strconv.ParseFloat(high, 64)
			var highFloat = float32(high64)

			low64, _ := strconv.ParseFloat(low, 64)
			var lowFloat = float32(low64)

			close64, _ := strconv.ParseFloat(close, 64)
			var closeFloat = float32(close64)





			err = c.Insert(&Data{baseName, dateFormat, openFloat, highFloat, lowFloat, closeFloat, lotFloat, volumeFloat})


		}




	}
}




func FilePathWalkDir(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		ext := filepath.Ext(path)
		if ext == ".DS_Store" || ext == ".iml" || ext == ".xml" {
			return nil
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func ReadCsv(f io.Reader) []string {
	var values []string
	r := csv.NewReader(bufio.NewReader(f))
	for {
		record, err := r.Read()

		if err == io.EOF {
			break
		}

		for value := range record {
			//fmt.Printf("  %v\n", record[value])
			values = append(values, record[value])
		}
	}

	return values
}