package oracli

import goOra "github.com/sijms/go-ora/v2"

// buildParamsList takes a list of @Param and convert to a object
// of parameters recognized by go_ora to allow replacement
// Parameters:
// @parameters List of parameters to convert
func buildParamsListWithClob(parameters []*Param) (*params, *goOra.Clob) {
	l := &params{}
	var v []any
	var data goOra.Clob

	for _, p := range parameters {

		// if direction is Clob is and output Param of type Clob
		if p.Direction == Clob {
			l.isClob = true
			v = append(v, goOra.Out{Dest: &data, Size: p.Size})
			continue
		}

		v = append(v, p.Value)
	}

	l.values = v

	return l, &data
}

// buildParamsList takes a list of @Param and convert to a object
// of parameters recognized by go_ora to allow replacement
// Parameters:
// @parameters List of parameters to convert
func buildParamsList(parameters []*Param) *params {
	l := &params{}
	var v []any
	var cursor goOra.RefCursor
	var data goOra.Clob

	for _, p := range parameters {

		// for cursors a goOra.RefCursor is neeeded
		if p.IsRef {
			l.isRef = true
			l.cursor = &cursor
			v = append(v, goOra.Out{Dest: l.cursor})
			continue
		}

		// if direction is Clob is and output Param of type Clob
		if p.Direction == Clob {
			l.isClob = true
			v = append(v, goOra.Out{Dest: &data, Size: p.Size})
			continue
		}

		v = append(v, p.Value)
	}

	l.values = v

	return l
}

// unwrapToRecord take every row and create a new Record
// Parameters:
// @columns Every column in the DataSet
// @values Every value in the DataSet
func unwrapToRecord(columns []string, values []any) Record {
	r := make(Record)
	for i, c := range values {
		r[columns[i]] = c
	}
	return r
}

// unwrapToRecord take every row and create a new Record
// Parameters:
// @columns Every column in the DataSet
// @values Every value in the Dataset (as string)
func unwrapToRecordString(columns []string, values []string) Record {
	r := make(Record)
	for i, c := range values {
		r[columns[i]] = c
	}
	return r
}
