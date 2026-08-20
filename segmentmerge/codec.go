package segmentmerge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

func DecodeManifest(data []byte) (Manifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("%w: decode: %v", ErrInvalidManifest, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Manifest{}, fmt.Errorf("%w: trailing JSON value", ErrInvalidManifest)
	}
	return CompileManifest(manifest)
}

func EncodeManifest(manifest Manifest) ([]byte, error) {
	compiled, err := CompileManifest(manifest)
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(compiled, "", "  ")
}
