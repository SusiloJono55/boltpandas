package boltpandas

import (
	"bolthold"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func initStore(path string) (*bolthold.Store, error) {
	db, err := bolthold.Open(path, 0666, nil)
	if err != nil {
		logrus.Panicf("failed to open db on path %s. Msg: %s", path, err)
		os.Exit(1)
	}
	return db, nil
}

func RandString() string {
	b1 := make([]byte, 5)
	b2 := make([]byte, 5)
	rand.Read(b1)
	rand.Read(b2)
	return fmt.Sprintf("%x%v%x", b1, time.Now().UnixNano(), b2)
}

func Test_toDf(t *testing.T) {
	filename := "./storage/qstore.db"
	// qStore, err = bolthold.Open(filename, 0666, nil)
	qStore, err := initStore(filename)

	if err != nil {
		panic(err)
	}
	db := qStore.Bolt()
	defer qStore.Close()
	defer os.Remove(filename)
	ids := []string{}
	for i := 0; i < 5; i++ {
		ids = append(ids, RandString())
	}
	data := []Dataset{
		Dataset{
			Quid:        "alfaromeo",
			UserID:      222,
			UserQueryID: 123,
			Source:      "file123",
			Columns:     `["_event","nama","kelamin","usia","IQ"]`,
			Rows:        `["aldi,bodi,24,111.2","aldi","","","111.2"]`,
			CreatedDate: time.Now(),
			UpdatedDate: time.Now(),
		},
		Dataset{
			Quid:        "alfaromeo",
			UserID:      222,
			UserQueryID: 123,
			Source:      "file123",
			Columns:     `["_event","nama","kelamin","usia","IQ"]`,
			Rows:        `["aldi|ana|s|111.2","badu","","40","99.27"]`,
			CreatedDate: time.Now(),
			UpdatedDate: time.Now(),
		},
		Dataset{
			Quid:        "alfaromeo",
			UserID:      222,
			UserQueryID: 123,
			Source:      "file123",
			Columns:     `["_event","nama","kelamin","usia","IQ"]`,
			Rows:        `["aldi|sule|24|111.2","kuming","wanita","22",""]`,
			CreatedDate: time.Now(),
			UpdatedDate: time.Now(),
		},
		Dataset{
			Quid:        "alfaromeo",
			UserID:      222,
			UserQueryID: 123,
			Source:      "file123",
			Columns:     `["_event","nama","kelamin","usia","IQ"]`,
			Rows:        `["aldi|alo|24|111.2","sirat","pria","","110.3"]`,
			CreatedDate: time.Now(),
			UpdatedDate: time.Now(),
		},
		Dataset{
			Quid:        "alfaromeo",
			UserID:      222,
			UserQueryID: 123,
			Source:      "file123",
			Columns:     `["_event","nama","kelamin","usia","IQ"]`,
			Rows:        `["aldi|aln|24|111.2","misrani","wanita","10","121.4"]`,
			CreatedDate: time.Now(),
			UpdatedDate: time.Now(),
		},
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		for i := range data {
			err := qStore.TxInsert(tx, ids[i], data[i])
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		// handle error
		fmt.Println(fmt.Errorf("%v", err))
	}

	// Find all items in specific Quid
	result := new(Datasets)

	query := bolthold.Where("Quid").Eq("alfaromeo")

	err = qStore.Find(&result.Ds, query)

	if err != nil {
		// handle error
		fmt.Println(fmt.Errorf("%v", err))
	}

	columnids := result.Ds[0].Columns
	fmt.Println(columnids)

	testDf := ToDf(columnids, result)

	fmt.Println(testDf)

	byGender := testDf.GroupBy("kelamin")

	fmt.Println(byGender.Min())
	fmt.Println(byGender.Max())
	fmt.Println(byGender.Max().Select("nama", "IQ"))

}

func Test_stringToSlice(t *testing.T) {
	data := `["aldi,bodi,24,111.2","aldi","","24","111.2"]`

	res := stringToSlice(data)
	fmt.Println(res)
}
