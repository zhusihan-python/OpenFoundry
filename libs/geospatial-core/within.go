package geospatialcore

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/peterstace/simplefeatures/geom"
)

// GeoJSONWithin tests planar containment in the existing WGS84 GeoJSON domain.
// It requires intersecting interiors and rejects any portion outside the container.
func GeoJSONWithin(candidate, container []byte) (bool, error) {
	left, err := withinGeometry(candidate)
	if err != nil {
		return false, fmt.Errorf("candidate geometry: %w", err)
	}
	right, err := withinGeometry(container)
	if err != nil {
		return false, fmt.Errorf("container geometry: %w", err)
	}
	return geom.Within(left, right)
}

func withinGeometry(raw []byte) (geom.Geometry, error) {
	geometry, err := ParseGeoJSONGeometry(raw)
	if err != nil {
		return geom.Geometry{}, err
	}
	if bytes.Contains(geometry.Coordinates, []byte("null")) {
		return geom.Geometry{}, errors.New("GeoJSON positions require numeric coordinates")
	}
	return geom.UnmarshalGeoJSON(raw)
}
