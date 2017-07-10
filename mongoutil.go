package mongoutil

import (
	"errors"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// InsertToDB insert a document to dburl -> db -> table -> doc -> error
// error will be returned if anything assocaited
// with db goes wrong
func InsertToDB(dbURL string, db string, table string, doc interface{}) error {

	session, err := mgo.Dial(dbURL)

	if err != nil {
		return err
	}

	if db == "" || table == "" {
		return errEmpty
	}

	defer session.Close()

	session.SetMode(mgo.Monotonic, true)

	col := session.DB(db).C(table)
	err = col.Insert(doc)
	return err
}

// UpdateDB updates a record given a key to find the record and new values to be replaced
// user can specify db url, db name and table name. If anything wrong happens on db
// session or during the update process error is returned.
// dburl -> db -> table -> key -> values -> error
func UpdateDB(dbURL string, db string, table string, key map[string]interface{}, values map[string]interface{}) error {

	session, err := mgo.Dial(dbURL)

	if err != nil {
		return err
	}

	if db == "" || table == "" {
		return errEmpty
	}

	defer session.Close()

	session.SetMode(mgo.Monotonic, true)

	col := session.DB(db).C(table)
	err = col.Update(key, bson.M{"$set": values})
	return err
}

// FindDB retrieves the record, user can indicate url for the db, db name, table name
// criteria to find the record, and if the record should be unique or not.
// if set to unique then only one record is retreived otherwise a list is returned
// if anything goes wrong with db connect or process to find data error is returned
// dburl -> db -> table -> key -> unique -> record, error
func FindDB(dbURL string, db string, table string, key interface{}, unique bool) (interface{}, error) {

	session, err := mgo.Dial(dbURL)

	if err != nil {
		return nil, err
	}

	if db == "" || table == "" {
		return nil, errEmpty
	}

	defer session.Close()

	session.SetMode(mgo.Monotonic, true)

	col := session.DB(db).C(table)

	var result interface{}
	if unique == true {
		err = col.Find(key).One(&result)
		return result, err
	}

	err = col.Find(key).All(&result)
	return result, err
}

// CheckExistInDB is a mongodb utility function which checks given a
// certain criteria, a record cound be found in db
// dburl -> dbname -> table -> query -> unique -> isExist, error
// if anything goes wrong with db or query, error will be returned
// if unique flag is set, only when one record found is legal, a true and
// nil of error will be returned, if more than one record is found, then a false
// and error will be returned.
func CheckExistInDB(dbURL string, db string, table string, query interface{}, unique bool) (bool, error) {

	session, err := mgo.Dial(dbURL)
	var count int

	if err != nil {
		return false, err
	}

	if db == "" || table == "" {
		return false, errEmpty
	}

	defer session.Close()

	session.SetMode(mgo.Monotonic, true)

	col := session.DB(db).C(table)
	count, err = col.Find(query).Count()

	if count == 1 {
		return true, nil
	} else if count == 0 {
		return false, nil
	} else {
		if unique {
			return false, errRecord
		}
		return true, nil
	}
}

var errEmpty = errors.New("empty string")

var errRecord = errors.New("illegal records")
