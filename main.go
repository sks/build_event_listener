package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	pq "github.com/lib/pq"
)

var processedTillRow int

func main() {

	conninfo := "postgres://concourse:changeme@localhost/concourse?sslmode=disable"

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
	buildEventId   int
	buildeventType string
	payload        string
	eventId        int
	version        string
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
	buildEvent := buildEvent{}
	for rows.Next() {
		err := rows.Scan(&buildEvent.buildEventId, &buildEvent.buildeventType, &buildEvent.payload, &buildEvent.eventId, &buildEvent.version)
		if err != nil {
			log.Printf("[ERR] error decoding the row %+v", rows, err)
			continue
		}
		err = processBuildEvent(buildEvent)
		if err != nil {
			log.Printf("[ERR] error processing the row %+v", rows, err)
			continue
		}
	}

	processedTillRow = totalRows
	// work here
}

func processBuildEvent(buildEvent buildEvent) error {
	log.Printf("%+v", buildEvent)
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
			go doWork(db, totalRows)
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
