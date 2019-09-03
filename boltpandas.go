package boltpandas

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/sapariduo/gopandas/dataframes"
	"github.com/sapariduo/gopandas/series"
	"github.com/sapariduo/gopandas/types"

	"github.com/araddon/dateparse"
)

type Dataset struct {
	ID          string    `boltholdKey:"ID"`
	Quid        string    `json:"quid" boltholdIndex:"Quid"` // paques quid
	UserID      string    `json:"userID"`
	UserQueryID uint64    `json:"userQueryID"`
	Source      string    `json:"source"`  // paques rset source
	Columns     string    `json:"columns"` // paques rset columns
	Rows        string    `json:"rows"`    // paques rset rows
	CreatedDate time.Time `json:"createdDate"`
	UpdatedDate time.Time `json:"updatedDate"`
}

type Type string

type Datasets struct {
	Ds      []Dataset
	ColType map[string]Type
}

type chanSeries struct {
	column string
	data   *series.Series
	err    error
}

const (
	NUMERIC Type = "numeric"
	TIME         = "time"
	STRING       = "string"
)

func stringToSlice(str1 string) []string {
	quoteRemover := regexp.MustCompile(`\\"(.*?)\\"`)
	re := regexp.MustCompile(`(".*?")|([0-9.]+)|null`)
	match := re.FindAllString(quoteRemover.ReplaceAllString(str1, "$1"), -1)
	return match

}

func typeString(obj []byte) bool {
	pattern := regexp.MustCompile(`[a-zA-Z]`)
	loc := pattern.FindIndex(obj)
	if len(loc) > 0 {
		return true
	} else {
		return false
	}
}

func typeInt(obj []byte) bool {
	pattern0 := regexp.MustCompile(`\d`)
	pattern1 := regexp.MustCompile(`\.`)
	loc0 := pattern0.FindIndex(obj)
	loc1 := pattern1.FindIndex(obj)
	if !typeString(obj) && len(loc0) > 0 && len(loc1) == 0 {
		return true
	} else {
		return false
	}
}

func typeFloat(obj []byte) bool {
	pattern0 := regexp.MustCompile(`\d`)
	pattern1 := regexp.MustCompile(`\.`)
	loc0 := pattern0.FindIndex(obj)
	loc1 := pattern1.FindIndex(obj)
	if !typeString(obj) && len(loc0) > 0 && len(loc1) > 0 {
		return true
	} else {
		return false
	}
}

func typeIdent(obj []byte) interface{} {
	if typeString(obj) {
		return string(obj)
	} else if typeInt(obj) {
		i, err := strconv.Atoi(string(obj))
		if err != nil {
			return err
		}
		return i
	} else if typeFloat(obj) {
		i, err := strconv.ParseFloat(string(obj), 64)
		if err != nil {
			return err
		}
		return i
	} else {
		return nil
	}
}

func seriesInt(slot []int, datasets *Datasets, colName string, colIndex int) (*series.Series, error) {
	srx := series.NewEmpty()

	for i := range slot {
		record := stringToSlice(datasets.Ds[i].Rows)
		strvalue := record[colIndex]

		switch len(strvalue) {
		case 2:
			// slot[i] = int(0)
			srx.Set(i, types.NewNan())
		default:
			value := []byte(strvalue[1 : len(strvalue)-1])
			intValue, err := strconv.Atoi(string(bytes.TrimSpace(value)))
			if err != nil {
				return nil, fmt.Errorf("seriesInt, wrong type for column %s, row %d, expected int", colName, i)
			}
			// slot[i] = intValue
			srx.Set(i, types.Numeric(float64(intValue)))
		}

	}
	// sr := series.New(slot)
	return srx, nil
}

func seriesFloat(slot []float64, records [][]string, colName string, colIndex int) (srx *series.Series, err error) {
	var i int

	defer func() {
		if recover() != nil {
			fmt.Printf("seriesFloat PANIC DATASET (%d): %v, RECORD: %v, RECORD IDX: %d\n", colIndex, records[i][colIndex], records[i], i)
			err = errors.New("seriesFloat panic")
		}
	}()

	srx = series.NewEmpty()
	for i = range slot {
		strvalue := records[i][colIndex]
		if strvalue[0] != '"' {
			floatValue, _ := strconv.ParseFloat(string(strvalue), 64)
			srx.Set(i, types.Numeric(floatValue))
		} else if strvalue == "null" {
			srx.Set(i, types.NewNan())
		} else {
			switch len(strvalue) {
			case 2:
				// slot[i] = float64(0)
				srx.Set(i, types.NewNan())
			default:
				value := []byte(strvalue[1 : len(strvalue)-1])
				floatValue, err := strconv.ParseFloat(string(bytes.TrimSpace(value)), 64)
				if err != nil {
					fmt.Printf("ERROR VALUE: %v", strvalue)
					return nil, fmt.Errorf("seriesFloat, wrong type for column %s, row %d, expected float/int", colName, i)
				}
				// slot[i] = floatValue
				srx.Set(i, types.Numeric(floatValue))
			}
		}

	}
	// sr := series.New(slot)
	return srx, nil
}

func seriesString(col []string, records [][]string, colName string, colIndex int) (srx *series.Series, err error) {
	var i int

	defer func() {
		if recover() != nil {
			fmt.Printf("seriesString PANIC DATASET (%d): %v, RECORD: %v, RECORD IDX: %d\n", colIndex, records[i][colIndex], records[i], i)
			err = errors.New("seriesString panic")
		}
	}()

	srx = series.NewEmpty()
	for i = range col {
		var value []byte
		strvalue := records[i][colIndex]
		if len(strvalue) > 1 {
			value = []byte(strvalue[1 : len(strvalue)-1])
		} else {
			value = []byte(strvalue)
		}

		if strvalue[0] != '"' {
			str := string(bytes.TrimSpace(value))
			srx.Set(i, types.String(str))
		} else if strvalue == "null" {
			srx.Set(i, types.NewNan())
		} else {
			if len(value) == 0 {
				srx.Set(i, types.NewNan())
			} else {
				str := string(bytes.TrimSpace(value))
				srx.Set(i, types.String(str))
			}
		}
	}

	// sr := series.New(col)
	return srx, nil
}

func seriesTime(col []string, records [][]string, colName string, colIndex int) (srx *series.Series, err error) {
	var i int

	defer func() {
		if recover() != nil {
			fmt.Printf("seriesTime PANIC DATASET (%d): %v, RECORD: %v, RECORD IDX: %d\n", colIndex, records[i][colIndex], records[i], i)
			err = errors.New("seriesTime panic")
		}
	}()

	srx = series.NewEmpty()
	for i = range col {
		strvalue := records[i][colIndex]
		value := []byte(strvalue[1 : len(strvalue)-1])

		if len(value) == 0 || strvalue == "null" {
			srx.Set(i, types.NewNan())
		} else {
			timeVal, _ := dateparse.ParseAny(string(value))
			srx.Set(i, types.Time(timeVal))
		}

	}

	return srx, nil
}

func ToDf(columnids string, datasets *Datasets) (*dataframes.DataFrame, error) {
	rawColName := stringToSlice(columnids)
	var columns []string
	var wg sync.WaitGroup

	for _, vals := range rawColName {
		record := []byte(vals[1 : len(vals)-1])
		columns = append(columns, string(record))
	}

	//create empty new dataframe
	df := dataframes.NewEmpty()

	dsLen := len(datasets.Ds)
	records := make([][]string, dsLen)
	for i := range records {
		sts := stringToSlice(datasets.Ds[i].Rows)
		if len(sts) != len(rawColName) {
			continue
		}

		records[i] = sts
	}

	srs := make(chan *chanSeries, len(columns))
	wg.Add(len(columns))

	for colIndex := range columns {
		go func(x int) {
			defer wg.Done()
			chvalue := &chanSeries{column: columns[x]}
			switch datasets.ColType[columns[x]] {

			case NUMERIC:
				col := make([]float64, len(records))
				sr, err := seriesFloat(col, records, columns[x], x)
				chvalue.data = sr
				chvalue.err = err
			case STRING:
				col := make([]string, len(records))
				sr, err := seriesString(col, records, columns[x], x)
				chvalue.data = sr
				chvalue.err = err
			case TIME:
				col := make([]string, len(records))
				sr, err := seriesTime(col, records, columns[x], x)
				chvalue.data = sr
				chvalue.err = err
			default:
				chvalue.err = fmt.Errorf("Unknown data type")

			}

			srs <- chvalue
		}(colIndex)
	}

	wg.Wait()

	for y := 0; y < cap(srs); y++ {
		chans := <-srs
		if chans.err != nil {
			return nil, chans.err
		}
		df.AddSeries(chans.column, chans.data)
	}

	return df, nil
}
