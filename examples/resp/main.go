package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"reflect"

	"github.com/mediocregopher/radix/v3/resp"
	r "github.com/mediocregopher/radix/v3/resp/resp2"
)

func main() {

	newLR := func(s string) resp.LenReader {
		buf := bytes.NewBufferString(s)
		return resp.NewLenReader(buf, int64(buf.Len()))
	}

	type encodeTest struct {
		in  resp.Marshaler
		out string

		errStr bool
	}

	encodeTests := func() []encodeTest {
		return []encodeTest{
			{in: &r.SimpleString{S: ""}, out: "+\r\n"},
			{in: &r.SimpleString{S: "foo"}, out: "+foo\r\n"},
			{in: &r.Error{E: errors.New("")}, out: "-\r\n", errStr: true},
			{in: &r.Error{E: errors.New("foo")}, out: "-foo\r\n", errStr: true},
			{in: &r.Int{I: 5}, out: ":5\r\n"},
			{in: &r.Int{I: 0}, out: ":0\r\n"},
			{in: &r.Int{I: -5}, out: ":-5\r\n"},
			{in: &r.BulkStringBytes{B: nil}, out: "$-1\r\n"},
			{in: &r.BulkStringBytes{B: []byte{}}, out: "$0\r\n\r\n"},
			{in: &r.BulkStringBytes{B: []byte("foo")}, out: "$3\r\nfoo\r\n"},
			{in: &r.BulkStringBytes{B: []byte("foo\r\nbar")}, out: "$8\r\nfoo\r\nbar\r\n"},
			{in: &r.BulkString{S: ""}, out: "$0\r\n\r\n"},
			{in: &r.BulkString{S: "foo"}, out: "$3\r\nfoo\r\n"},
			{in: &r.BulkString{S: "foo\r\nbar"}, out: "$8\r\nfoo\r\nbar\r\n"},
			{in: &r.BulkReader{LR: newLR("foo\r\nbar")}, out: "$8\r\nfoo\r\nbar\r\n"},
			{in: &r.ArrayHeader{N: 5}, out: "*5\r\n"},
			{in: &r.ArrayHeader{N: -1}, out: "*-1\r\n"},
			{in: &r.Array{}, out: "*-1\r\n"},
			{in: &r.Array{A: []resp.Marshaler{}}, out: "*0\r\n"},
			{
				in: &r.Array{A: []resp.Marshaler{
					r.SimpleString{S: "foo"},
					r.Int{I: 5},
				}},
				out: "*2\r\n+foo\r\n:5\r\n",
			},
		}
	}

	for _, et := range encodeTests() {
		buf := new(bytes.Buffer)
		err := et.in.MarshalRESP(buf)
		if err != nil {
			panic(err)
		}

		br := bufio.NewReader(buf)
		umr := reflect.New(reflect.TypeOf(et.in).Elem())
		um, ok := umr.Interface().(resp.Unmarshaler)
		if !ok {
			_, err = br.Discard(len(et.out))
			if err != nil {
				panic(err)
			}
			continue
		}

		err = um.UnmarshalRESP(br)
		if err != nil {
			panic(err)
		}

		var exp interface{} = et.in
		var got interface{} = umr.Interface()
		if et.errStr {
			exp = exp.(error).Error()
			got = got.(error).Error()
		}
		fmt.Printf("exp:%#v got:%#v\n", exp, got)
	}

	// // Same test, but do all the marshals first, then do all the unmarshals
	// {
	// 	ett := encodeTests()
	// 	buf := new(bytes.Buffer)
	// 	for _, et := range ett {
	// 		assert.Nil(t, et.in.MarshalRESP(buf))
	// 	}
	// 	br := bufio.NewReader(buf)
	// 	for _, et := range ett {
	// 		umr := reflect.New(reflect.TypeOf(et.in).Elem())
	// 		um, ok := umr.Interface().(resp.Unmarshaler)
	// 		if !ok {
	// 			_, err := br.Discard(len(et.out))
	// 			assert.NoError(t, err)
	// 			continue
	// 		}

	// 		err := um.UnmarshalRESP(br)
	// 		assert.Nil(t, err)

	// 		var exp interface{} = et.in
	// 		var got interface{} = umr.Interface()
	// 		if et.errStr {
	// 			exp = exp.(error).Error()
	// 			got = got.(error).Error()
	// 		}
	// 		assert.Equal(t, exp, got, "exp:%#v got:%#v", exp, got)
	// 	}
	// }
}
