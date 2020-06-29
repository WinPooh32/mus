package main

import (
	"encoding/csv"
	"flag"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/timshannon/bolthold"

	"github.com/WinPooh32/mus/model"
)

func mustOpenArtist(dir string) *os.File {
	file, err := os.Open(dir + "/artist")
	if err != nil {
		log.Fatal(err)
	}
	return file
}

func mustOpenWork(dir string) *os.File {
	file, err := os.Open(dir + "/work")
	if err != nil {
		log.Fatal(err)
	}
	return file
}

func mustOpenLinkArtistWork(dir string) *os.File {
	file, err := os.Open(dir + "/l_artist_work")
	if err != nil {
		log.Fatal(err)
	}
	return file
}

func newCsvReader(file *os.File) *csv.Reader {
	r := csv.NewReader(file)
	r.Comma = '\t'
	r.Comment = '#'
	r.LazyQuotes = true
	r.ReuseRecord = true
	r.FieldsPerRecord = -1

	return r
}

func next(r *csv.Reader) ([]string, bool) {
	record, err := r.Read()
	if err == io.EOF {
		return nil, false
	}
	if err != nil {
		log.Fatal(err)
	}
	return record, true
}

func insertArtists(store *bolthold.Store, r *csv.Reader) {
	const (
		fieldID       = 0
		fieldGID      = 1
		fieldName     = 2
		fieldNameSort = 3
	)

	for {
		record, ok := next(r)
		if !ok {
			break
		}

		id, err := strconv.ParseUint(record[fieldID], 10, 64)
		if err != nil {
			log.Fatal(err)
		}

		gid, err := uuid.Parse(record[fieldGID])
		if err != nil {
			log.Fatal(err)
		}

		name := record[fieldName]
		nameSort := record[fieldNameSort]

		artist := model.Artist{
			ID:       id,
			GID:      gid,
			Name:     name,
			NameSort: nameSort,
			Works:    nil,
		}

		err = store.Upsert(id, artist)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func insertWorks(store *bolthold.Store, r *csv.Reader) {
	const (
		fieldID   = 0
		fieldGID  = 1
		fieldName = 2
	)

	for {
		record, ok := next(r)
		if !ok {
			break
		}

		id, err := strconv.ParseUint(record[fieldID], 10, 64)
		if err != nil {
			log.Fatal(err)
		}

		gid, err := uuid.Parse(record[fieldGID])
		if err != nil {
			log.Fatal(err)
		}

		name := record[fieldName]

		work := model.Work{
			ID:   id,
			GID:  gid,
			Name: name,
		}

		err = store.Upsert(id, work)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func linkArtistsWorks(store *bolthold.Store, r *csv.Reader) {
	const (
		fieldArtistID = 2
		fieldWorkID   = 3
	)

	for {
		record, ok := next(r)
		if !ok {
			break
		}

		artistID, err := strconv.ParseUint(record[fieldArtistID], 10, 64)
		if err != nil {
			log.Fatal(err)
		}

		workID, err := strconv.ParseUint(record[fieldWorkID], 10, 64)
		if err != nil {
			log.Fatal(err)
		}

		var artist model.Artist
		err = store.Get(artistID, &artist)
		if err != nil {
			log.Fatal(err)
		}

		artist.Works = append(artist.Works, workID)

		err = store.Update(artistID, artist)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	var dir string

	flag.StringVar(&dir, "dir", ".", "database dump")
	flag.Parse()

	artist := mustOpenArtist(dir)
	defer artist.Close()

	work := mustOpenWork(dir)
	defer work.Close()

	link := mustOpenLinkArtistWork(dir)
	defer link.Close()

	rArtist := newCsvReader(artist)
	rWork := newCsvReader(work)
	rLink := newCsvReader(link)

	store, err := bolthold.Open("mus.db", 0666, nil)
	if err != nil {
		panic(err)
	}

	t := time.Now()
	log.Println("Insert artists...")
	insertArtists(store, rArtist)
	log.Println("Elapsed:", time.Since(t))

	t = time.Now()
	log.Println("Insert works...")
	insertWorks(store, rWork)
	log.Println("Elapsed:", time.Since(t))

	t = time.Now()
	log.Println("Link artists works...")
	linkArtistsWorks(store, rLink)
	log.Println("Elapsed:", time.Since(t))

	log.Println("Done")
}
