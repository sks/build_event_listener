package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	pq "github.com/lib/pq"
)

var (
	processedTillRow      int
	conninfo              = "postgres://concourse:changeme@localhost/concourse?sslmode=disable"
	elasticSearchEndpoint = os.Getenv("ELASTICSEARCH_ENDPOINT")
)

func main() {

	db, err := sql.Open("postgres", conninfo)
	if err != nil {
		log.Fatal(err)
	}

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	listener := pq.NewListener(conninfo, 10*time.Second, time.Minute, reportProblem)
	err = listener.Listen("new_build_events")
	if err != nil {
		panic(err)
	}

	for {
		// process all available work before waiting for notifications
		checkForWork(db)
		waitForNotification(listener)
	}
}

type buildEvent struct {
	BuildEventId   int    `json:"build_event_id"`
	BuildeventType string `json:"build_event_type"`
	Payload        string `json:"payload"`
	EventId        int    `json:"event_id"`
	Version        string `json:"version"`
}

func doWork(db *sql.DB, totalRows int) {
	log.Printf("need to process build between %d and %d ", processedTillRow, totalRows)
	query := fmt.Sprintf("Select * from build_events order by build_id, event_id OFFSET %d limit %d", processedTillRow, (totalRows - processedTillRow))
	rows, err := db.Query(query)
	if err != nil {
		log.Printf("[ERR] Error getting the latest rows", err)
		return
	}
	defer rows.Close()
	index := processedTillRow

	buildEvent := buildEvent{}
	for rows.Next() {
		err := rows.Scan(&buildEvent.BuildEventId, &buildEvent.BuildeventType, &buildEvent.Payload, &buildEvent.EventId, &buildEvent.Version)
		if err != nil {
			log.Printf("[ERR] error decoding the row %+v", rows, err)
			continue
		}
		index++
		err = processBuildEvent(index, buildEvent)
		if err != nil {
			log.Printf("[ERR] error processing the row %+v", rows, err)
			continue
		}
	}

	processedTillRow = totalRows
	// work here
}

func processBuildEvent(index int, buildEvent buildEvent) error {
	url := fmt.Sprintf("%s/%s/build_event/%d", elasticSearchEndpoint, os.Getenv("ELASTICSEARCH_BUILD_NODE"), index)
	jsonStr, err := json.Marshal(buildEvent)
	if err != nil {
		log.Println("[ERR] creating request body", err)
		return err
	}
	log.Printf("Processing the build id : %d and build event: %d", buildEvent.BuildEventId, buildEvent.EventId)
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		log.Fatal("Creating the new request failed: ", err)
		return err
	}
	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[ERR] error sending the build event %+v to Elastic Search %s: %s", buildEvent, url, err)
		return err
	}
	defer resp.Body.Close()
	fmt.Println("response Status:", resp.Status)
	fmt.Println("response Headers:", resp.Header)
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[ERR] Error reading the response", err)
		return err
	}
	log.Println(string(responseBody))
	return nil
}

func checkForWork(db *sql.DB) {
	for {
		var totalRows int
		err := db.QueryRow("SELECT count(1) from build_events").Scan(&totalRows)
		if err != nil {
			log.Printf("[ERR] Could not get the max build number", err)
			time.Sleep(10 * time.Second)
			continue
		}
		if processedTillRow < totalRows {
			doWork(db, totalRows)
		}
	}
}

func waitForNotification(l *pq.Listener) {
	for {
		select {
		case <-l.Notify:
			log.Println("received notification, new work available")
			return
		case <-time.After(90 * time.Second):
			go func() {
				l.Ping()
			}()
			// Check if there's more work available, just in case it takes
			// a while for the Listener to notice connection loss and
			// reconnect.
			log.Println("received no work for 90 seconds, checking for new work")
			return
		}
	}
}
