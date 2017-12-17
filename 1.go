package main

import (
    "fmt"
    "os"
    "flag"
    "io"
    "strings"
    "bufio"
    "time"
    "net/http"
    "errors"
)

type Resource interface{}
type Resources map[string]Resource

type ReadCloser interface {
    io.Reader
    Close() error
}

type GetReaderFn func(string, Resource) (ReadCloser, error)
type Readers map[string]GetReaderFn


func debug_reader(source string, unused Resource) (ReadCloser, error) {
    return &Src{0, "Goreading Gofrom Go file: Go" + source}, nil
}

func file_reader(source string, unsued Resource) (ReadCloser, error) {
    f, err := os.Open(source)
    return f, err
}

// don't forget to timeout http requests
func url_reader(source string, r Resource) (ReadCloser, error) {
    http_client, ok := r.(*http.Client)
    if !ok {
        return nil, errors.New("wrong type: expected http.Client type")
    }

    res, err := http_client.Get(source)

    if err != nil {
        return nil, err
    }
    body := res.Body
    return body, nil
}

func get_url_resource() Resource {
    // set time out for request
    request_timeout := 5 * time.Second
    // leave default redirect policy: 10 hops
    client := &http.Client{
                    Timeout: request_timeout,
    }
    return client
}

// --- devel stuff for testing ---
type Src struct {
    cur_pos int
    text string
}

func (s *Src) Read(p []byte) (n int, err error) {
    // make it slow
    time.Sleep(1*time.Second)

    lt := len(s.text)

    // everything has been read
    if (lt == s.cur_pos) {
        return 0, io.EOF
    }

    lp := len(p)

    if (lt - s.cur_pos > lp) {
        new_pos := s.cur_pos + lp
        copy(p, s.text[s.cur_pos:new_pos])
        s.cur_pos = new_pos
        return lp, nil
    } else {
        copy(p, s.text[s.cur_pos:])
        s.cur_pos = lt
        return lt, io.EOF
    }
}

func (s *Src) Close() error {
    return nil
}
// -------------------

func exit_with_error(error_text string) {
    fmt.Fprintf(os.Stderr, "%s\n", error_text)
    fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
    flag.PrintDefaults()
    os.Exit(1)
}

func list_types(c Readers) string {
    // don't use super fast bytes.Buffer here
    // for simplicity and convenience.
    // we expect only a few items to iterate over
    s := []string{}
    for k, _ := range c {
        s = append(s, k)
    }

    return strings.Join(s, ", ")
}

func input_reader(ch_input chan<- string, ch_done chan<- bool,
                  ch_go_on <-chan bool) {
    std_reader := bufio.NewReader(os.Stdin)
    for {
        <-ch_go_on
        source, err := std_reader.ReadString('\n')
        if err != nil {
            if err != io.EOF {
                fmt.Fprintf(os.Stderr, "[Error] on stdin reading: %s\n", err.Error())
            }
            break
        }
        ch_input <- source
    }
    ch_done <- true
}

func word_counter(word string, source string, resource Resource,
                  get_reader GetReaderFn, ch_count chan<- uint) {
    var count uint

    src := strings.TrimSpace(source)

    if len(src) == 0 {
        ch_count <- 0
        return
    }

    rc, err := get_reader(src, resource)

    switch {
        case err != nil:
            fmt.Fprintf(os.Stderr, "[Error][%s] on getting reader: %s\n", src, err.Error())
        case rc == nil:
            // paraniod check (assertion) -- if someone added a new type unproperly
            // this would give a hint what is wrong
            fmt.Fprintf(os.Stderr, "[Error][%s] no reader\n", src)
        default:
            buf_reader := bufio.NewReader(rc)

            var s string
            var err error

            for {
                s, err = buf_reader.ReadString(' ')

                if err != nil {
                    if err == io.EOF {
                        count += uint(strings.Count(s, word))
                    } else {
                        fmt.Fprintf(os.Stderr, "[Error][%s] on string reading: %s\n", src, err.Error())
                    }
                    break
                }
                count += uint(strings.Count(s, word))
            }

            err = rc.Close()
            if err != nil {
                fmt.Fprintf(os.Stderr, "[Error][%s] on reader closing: %s\n", src, err.Error())
            }
    }

    fmt.Fprintf(os.Stdout, "Count for %s: %d\n", src, count)

    ch_count <- count
}

func main() {
    word := "Go"
    in_flight_limit := 5  // "k" in the assignment text

    readers := make(Readers);
    readers["file"] = file_reader
    readers["url"] = url_reader
    readers["debug"] = debug_reader

    // for every reader there should be a corresponiding
    // resource 
    resources := make(Resources);
    resources["file"] = nil
    resources["url"] = get_url_resource()
    resources["debug"] = nil

    user_type := flag.String("type", "", "type of input. supported types: " +
                     list_types(readers))
    flag.Parse()

    if len(*user_type) == 0 {
        exit_with_error("type should be specified")
    }

    reader, ok := readers[*user_type]
    if !ok {
        error_text := "type '" + *user_type + "' isn't supported"
        exit_with_error(error_text)
    }

    resource, ok := resources[*user_type]
    if !ok {
        error_text := "there is no resourse assgned for: '" + *user_type
        exit_with_error(error_text)
    }

    ch_input_reader_done := make(chan bool)
    ch_input := make(chan string, 1)
    ch_input_go_on := make(chan bool)

    go input_reader(ch_input, ch_input_reader_done, ch_input_go_on)
    ch_input_go_on<-true

    ch_word_count := make(chan uint)

    var total uint

    req_in_flight := 0
    has_input := true

    var tmp_input string

    for has_input || req_in_flight != 0 {
        select {
            case input := <-ch_input:
                if (req_in_flight != in_flight_limit) {
                    ch_input_go_on <- true
                    go word_counter(word, input, resource, reader, ch_word_count)
                    req_in_flight += 1
                } else {
                    tmp_input = input
                    // from this point the input reader will wait
                    // until we consume tmp_input by sending it
                    // via ch_input back here when req_in_flight drops 
                    // below the limit
                }
            case <-ch_input_reader_done:
                has_input = false
            case word_count := <-ch_word_count:
                total += word_count
                req_in_flight -= 1
                // a little bit ugly but we need only one string here
                // no need to use fancy data structures: stack, list
                if (len(tmp_input) > 0) {
                    ch_input <- tmp_input
                    tmp_input = ""
                }
        }
    }

    fmt.Fprintf(os.Stdout, "Total: %d\n", total)
}
