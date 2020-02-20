package service

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	. "gear/influx"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/prometheus"
	"github.com/influxdata/influxdb/prometheus/remote"
	"io"
	"net/http"
	"strconv"
	"time"
)

type ResponseWriter interface {
	// WriteResponse writes a response.
	WriteResponse(resp Response) (int, error)

	http.ResponseWriter
}

func NewResponseWriter(w http.ResponseWriter, r *http.Request) ResponseWriter {
	pretty := r.URL.Query().Get("pretty") == "true"
	rw := &responseWriter{ResponseWriter: w}
	switch r.Header.Get("Accept") {
	case "application/csv", "text/csv":
		w.Header().Add("Content-Type", "text/csv")
		rw.formatter = &csvFormatter{statementID: -1}
	case "application/x-msgpack":
		w.Header().Add("Content-Type", "application/x-protobuf")
		w.Header().Add("Content-Encoding", "snappy")
		rw.formatter = &protoFormatter{}
	case "application/json":
		fallthrough
	default:
		w.Header().Add("Content-Type", "application/json")
		rw.formatter = &jsonFormatter{Pretty: pretty}
	}
	return rw
}

type responseWriter struct {
	formatter interface {
		WriteResponse(w io.Writer, resp Response) error
	}
	http.ResponseWriter
}

type bytesCountWriter struct {
	w io.Writer
	n int
}

func (w *bytesCountWriter) Write(data []byte) (int, error) {
	n, err := w.w.Write(data)
	w.n += n
	return n, err
}

func (w *responseWriter) WriteResponse(resp Response) (int, error) {
	writer := bytesCountWriter{w: w.ResponseWriter}
	err := w.formatter.WriteResponse(&writer, resp)
	return writer.n, err
}

type jsonFormatter struct {
	Pretty bool
}

func (f *jsonFormatter) WriteResponse(w io.Writer, resp Response) (err error) {
	var b []byte
	if f.Pretty {
		b, err = json.MarshalIndent(resp, "", "    ")
	} else {
		b, err = json.Marshal(resp)
	}

	if err != nil {
		_, err = io.WriteString(w, err.Error())
	} else {
		_, err = w.Write(b)
	}

	//w.Write([]byte("\n"))
	return err
}

type csvFormatter struct {
	statementID int
	columns     []string
}

func (f *csvFormatter) WriteResponse(w io.Writer, resp Response) (err error) {
	csv := csv.NewWriter(w)
	if resp.Error != nil {
		csv.Write([]string{"error"})
		csv.Write([]string{resp.Error.Error()})
		csv.Flush()
		return csv.Error()
	}

	for _, result := range resp.Results {
		if result.StatementID != f.statementID {
			// If there are no series in the result, skip past this result.
			if len(result.Series) == 0 {
				continue
			}

			// Set the statement id and print out a newline if this is not the first statement.
			if f.statementID >= 0 {
				// Flush the csv writer and write a newline.
				csv.Flush()
				if err := csv.Error(); err != nil {
					return err
				}

				if _, err := io.WriteString(w, "\n"); err != nil {
					return err
				}
			}
			f.statementID = result.StatementID

			// Print out the column headers from the first series.
			f.columns = make([]string, 2+len(result.Series[0].Columns))
			f.columns[0] = "name"
			f.columns[1] = "tags"
			copy(f.columns[2:], result.Series[0].Columns)
			if err := csv.Write(f.columns); err != nil {
				return err
			}
		}

		for i, row := range result.Series {
			if i > 0 && !stringsEqual(result.Series[i-1].Columns, row.Columns) {
				// The columns have changed. Print a newline and reprint the header.
				csv.Flush()
				if err := csv.Error(); err != nil {
					return err
				}

				if _, err := io.WriteString(w, "\n"); err != nil {
					return err
				}

				f.columns = make([]string, 2+len(row.Columns))
				f.columns[0] = "name"
				f.columns[1] = "tags"
				copy(f.columns[2:], row.Columns)
				if err := csv.Write(f.columns); err != nil {
					return err
				}
			}

			f.columns[0] = row.Name
			f.columns[1] = ""
			if len(row.Tags) > 0 {
				hashKey := models.NewTags(row.Tags).HashKey()
				if len(hashKey) > 0 {
					f.columns[1] = string(hashKey[1:])
				}
			}
			for _, values := range row.Values {
				for i, value := range values {
					if value == nil {
						f.columns[i+2] = ""
						continue
					}

					switch v := value.(type) {
					case float64:
						f.columns[i+2] = strconv.FormatFloat(v, 'f', -1, 64)
					case int64:
						f.columns[i+2] = strconv.FormatInt(v, 10)
					case uint64:
						f.columns[i+2] = strconv.FormatUint(v, 10)
					case string:
						f.columns[i+2] = v
					case bool:
						if v {
							f.columns[i+2] = "true"
						} else {
							f.columns[i+2] = "false"
						}
					case time.Time:
						f.columns[i+2] = strconv.FormatInt(v.UnixNano(), 10)
					case *float64, *int64, *string, *bool:
						f.columns[i+2] = ""
					}
				}
				csv.Write(f.columns)
			}
		}
	}
	csv.Flush()
	return csv.Error()
}

type protoFormatter struct {}

func (f *protoFormatter) WriteResponse(w io.Writer, resp Response) (err error) {
	promResp := &remote.ReadResponse{
		Results: []*remote.QueryResult{{}},
	}

	for _, r := range resp.Results {
		// read the series data and convert into Prometheus samples
		for _, s := range r.Series {
			ts := &remote.TimeSeries{
				Labels: prometheus.TagsToLabelPairs(s.Tags),
			}

			for _, v := range s.Values {
				t, ok := v[0].(float64)
				if !ok {
					return errors.New("value wasn't a time")
				}
				timestamp := int64(t)
				value, ok := v[1].(float64)
				if !ok {
					return errors.New("value wasn't a float64")
				}
				ts.Samples = append(ts.Samples, &remote.Sample{
					TimestampMs: timestamp,
					Value:       value,
				})
			}

			promResp.Results[0].Timeseries = append(promResp.Results[0].Timeseries, ts)
		}
	}

	data, err := proto.Marshal(promResp)
	if err != nil {
		return
	}

	compressed := snappy.Encode(nil, data)
	_, err = w.Write(compressed)
	return
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
