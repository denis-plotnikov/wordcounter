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

type DebugSource struct {
    cur_pos int
    text string
}

func (s *DebugSource) Read(p []byte) (n int, err error) {
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

func (s *DebugSource) Close() error {
    return nil
}


type ReaderFactory interface {
    Init()
    GetReader(string) (io.ReadCloser, error)
}

type DebugReaderFactory struct {}

func (d *DebugReaderFactory) Init() {
}

func (d *DebugReaderFactory) GetReader(foo string) (io.ReadCloser, error) {
    return &DebugSource{0, "Text to find Go instance"}, nil
}


type FileReaderFactory struct {
}

func (frf *FileReaderFactory) Init() {
}

func (frf *FileReaderFactory) GetReader(name string) (io.ReadCloser, error) {
    if len(name) == 0 {
        return nil, errors.New("FileReaderFactory: file name is empty")
    }

    file, err := os.Open(name)
    return file, err
}

type URLReaderFactory struct {
   client *http.Client
}

func (urf *URLReaderFactory) Init() {
    request_timeout := 5 * time.Second
    // leave default redirect policy: 10 hops
    urf.client = &http.Client{
                         Timeout: request_timeout,
               }
}

func (urf *URLReaderFactory) GetReader(url string) (io.ReadCloser, error) {
    if len(url) == 0 {
        return nil, errors.New("URLReaderFactory: Open: url is empty")
    }

    res, err := urf.client.Get(url)

    if err != nil {
        return nil, err
    }

    return res.Body, nil
}

func exit_with_error(error_text string) {
    fmt.Fprintf(os.Stderr, "%s\n", error_text)
    fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
    flag.PrintDefaults()
    os.Exit(1)
}

func list_types() string {
        // keep types in the map of type "constructors"
	return "file, url, debug"
}

func input_reader(ch_input chan<- string, ch_done chan<- bool,
                  ch_go_on <-chan bool) {
    std_reader := bufio.NewReader(os.Stdin)
    for {
        <-ch_go_on
        source, err := std_reader.ReadString('\n')
        if err != nil {
            if err != io.EOF {
                fmt.Fprintf(os.Stderr, "[Error] on stdin reading: %s\n",
                            err.Error())
            }
            break
        }
        source = strings.TrimSpace(source)
        ch_input <- source
    }
    ch_done <- true
}

func count_words(word string, src string, reader_factory ReaderFactory,
                 ch_count chan<- uint) {
    var count uint
    defer func() {
              ch_count <- count
          }()

    if len(src) == 0 {
        return
    }

    defer func() {
              fmt.Printf("Count for %s: %d\n", src, count)
          }()

    reader, err := reader_factory.GetReader(src)
    if err != nil {
        fmt.Fprintf(os.Stderr, "[Error][%s] on getting reader: %s\n",
                    src, err.Error())
        return
    }

    // Close reader
    defer func() {
              err := reader.Close()
              if err != nil {
                  fmt.Fprintf(os.Stderr, "[Error][%s] on closing: %s\n",
                              src, err.Error())
              }
          }()

    buf_reader := bufio.NewReader(reader)

    var s string

    for {
        s, err = buf_reader.ReadString(' ')

        if err != nil {
            if err == io.EOF {
                count += uint(strings.Count(s, word))
            } else {
                fmt.Fprintf(os.Stderr, "[Error][%s] on string reading: %s\n",
                            src, err.Error())
            }
            break
        }
        count += uint(strings.Count(s, word))
    }
}

func main() {
    word := "Go"
    in_flight_limit := 5  // "k" in the assignment text

    user_type := flag.String("type", "", "type of input. supported types: " +
                             list_types())
    flag.Parse()

    if len(*user_type) == 0 {
        exit_with_error("type should be specified")
    }

    var reader_factory ReaderFactory

    switch *user_type {
    case "file":
        reader_factory = &FileReaderFactory{}
    case "url":
        reader_factory = &URLReaderFactory{}
    case "debug":
        reader_factory = &DebugReaderFactory{}
    default:
        exit_with_error("type '" + *user_type + "' isn't supported")
    }

    reader_factory.Init()

    ch_input_reader_done := make(chan bool)
    ch_input := make(chan string, 1)
    ch_input_go_on := make(chan bool)

    go input_reader(ch_input, ch_input_reader_done, ch_input_go_on)
    ch_input_go_on<-true

    ch_word_counter := make(chan uint)

    var total uint

    req_in_flight := 0
    has_input := true

    var tmp_input string

    for has_input || req_in_flight != 0 {
        select {
            case input := <-ch_input:
                if (req_in_flight != in_flight_limit) {
                    ch_input_go_on <- true
                    go count_words(word, input, reader_factory, ch_word_counter)
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
            case word_count := <-ch_word_counter:
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
