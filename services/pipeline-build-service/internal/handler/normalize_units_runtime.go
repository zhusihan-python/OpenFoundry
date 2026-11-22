package handler

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/openfoundry/openfoundry-go/libs/geospatial-core"
	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

type runtimeMeasurementUnit struct {
	id, label, dimension string
	scale, zero          float64
	aliases              []string
}

// Each unit maps to meters, kilograms or degrees Celsius via (input-zero)*scale.
var runtimeMeasurementUnits = []runtimeMeasurementUnit{
	{id: "mm", label: "Millimeters", dimension: "length", scale: 0.001, aliases: []string{"millimeter", "millimeters"}},
	{id: "cm", label: "Centimeters", dimension: "length", scale: 0.01, aliases: []string{"centimeter", "centimeters"}},
	{id: "m", label: "Meters", dimension: "length", scale: 1},
	{id: "km", label: "Kilometers", dimension: "length", scale: 1000},
	{id: "in", label: "Inches", dimension: "length", scale: 0.0254, aliases: []string{"inch", "inches"}},
	{id: "ft", label: "Feet", dimension: "length", scale: 0.3048, aliases: []string{"foot", "feet"}},
	{id: "yd", label: "Yards", dimension: "length", scale: 0.9144, aliases: []string{"yard", "yards"}},
	{id: "mi", label: "Miles", dimension: "length", scale: 1609.344},
	{id: "g", label: "Grams", dimension: "mass", scale: 0.001, aliases: []string{"gram", "grams"}},
	{id: "kg", label: "Kilograms", dimension: "mass", scale: 1, aliases: []string{"kilogram", "kilograms"}},
	{id: "oz", label: "Ounces", dimension: "mass", scale: 0.028349523125, aliases: []string{"ounce", "ounces"}},
	{id: "lb", label: "Pounds", dimension: "mass", scale: 0.45359237, aliases: []string{"pound", "pounds", "lbs"}},
	{id: "c", label: "Degrees Celsius", dimension: "temperature", scale: 1, aliases: []string{"celsius", "°c"}},
	{id: "f", label: "Degrees Fahrenheit", dimension: "temperature", scale: 5.0 / 9, zero: 32, aliases: []string{"fahrenheit", "°f"}},
	{id: "k", label: "Kelvin", dimension: "temperature", scale: 1, zero: 273.15, aliases: []string{"kelvin"}},
}

func normalizeUnitOptions() []pipelineTransformCatalogOption {
	options := make([]pipelineTransformCatalogOption, 0, len(runtimeMeasurementUnits))
	for _, unit := range runtimeMeasurementUnits {
		options = append(options, pipelineTransformCatalogOption{Value: unit.id, Label: unit.label})
	}
	return options
}

func lookupRuntimeUnit(raw string) (runtimeMeasurementUnit, error) {
	name := strings.ToLower(strings.TrimSpace(raw))
	// Keep the aliases already accepted by other PBS distance features.
	// The distance parser defaults blank to meters, but conversion requires a unit.
	if name != "" {
		distance, err := geospatialcore.ParseDistanceUnit(name)
		if err == nil {
			switch distance {
			case geospatialcore.DistanceUnitMeters:
				name = "m"
			case geospatialcore.DistanceUnitKilometers:
				name = "km"
			case geospatialcore.DistanceUnitMiles:
				name = "mi"
			}
		}
	}
	for _, unit := range runtimeMeasurementUnits {
		if name == unit.id || slices.Contains(unit.aliases, name) {
			return unit, nil
		}
	}
	return runtimeMeasurementUnit{}, fmt.Errorf("unsupported measurement unit %q", raw)
}

type runtimeUnitConversion struct {
	source, target       string
	sourceZero, destZero float64
	ratio                float64
	identity             bool
}

func validateRuntimeUnitConversion(block runtimeTransformBlock) (runtimeUnitConversion, error) {
	if strings.TrimSpace(block.SourceColumn) == "" {
		return runtimeUnitConversion{}, errors.New("unit conversion requires a source column")
	}
	source, err := lookupRuntimeUnit(block.SourceUnit)
	if err != nil {
		return runtimeUnitConversion{}, err
	}
	target, err := lookupRuntimeUnit(block.TargetUnit)
	if err != nil {
		return runtimeUnitConversion{}, err
	}
	if source.dimension != target.dimension {
		return runtimeUnitConversion{}, fmt.Errorf("cannot convert %s to %s: units must have the same dimension", source.id, target.id)
	}
	column := block.TargetColumn
	if strings.TrimSpace(column) == "" {
		column = block.SourceColumn
	}
	return runtimeUnitConversion{
		source: block.SourceColumn, target: column,
		sourceZero: source.zero, destZero: target.zero,
		ratio: source.scale / target.scale, identity: source.id == target.id,
	}, nil
}

func applyNormalizeUnitsSchema(nodeID string, schema pipelineStrictSchema, block runtimeTransformBlock, report *pipelineStrictValidationReport) pipelineStrictSchema {
	conversion, err := validateRuntimeUnitConversion(block)
	if err != nil {
		report.addError(nodeID, nil, "invalid_unit_conversion", err.Error())
		return schema
	}
	if !schema.Known {
		return schema
	}
	input, ok := schema.field(conversion.source)
	if !ok {
		report.addError(nodeID, strPtr(conversion.source), "missing_column", fmt.Sprintf("column %q does not exist in upstream schema", conversion.source))
		return schema
	}
	if !fieldType(input).IsNumeric() {
		report.addError(nodeID, strPtr(conversion.source), "invalid_unit_conversion_column", "unit conversion requires a numeric column")
		return schema
	}
	fields := cloneStrictFields(schema.Fields)
	output := pipelineStrictValidationField{Name: conversion.target, FieldType: "DOUBLE", Nullable: input.Nullable}
	for index, field := range fields {
		if field.Name == output.Name {
			fields[index] = output
			return pipelineStrictSchema{Known: true, Fields: fields}
		}
	}
	return pipelineStrictSchema{Known: true, Fields: append(fields, output)}
}

func applyRuntimeNormalizeUnitsBlock(rows []pe.Row, block runtimeTransformBlock) ([]pe.Row, error) {
	conversion, err := validateRuntimeUnitConversion(block)
	if err != nil {
		return nil, err
	}
	out := make([]pe.Row, 0, len(rows))
	for index, row := range rows {
		next := cloneRow(row)
		raw := bytes.TrimSpace(row[conversion.source])
		if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
			next[conversion.target] = pe.EvalNull().ToJSON()
			out = append(out, next)
			continue
		}
		value := pe.EvalValueFromJSON(raw)
		var number float64
		switch value.Kind {
		case pe.EvalKindInteger:
			number = float64(value.Int)
		case pe.EvalKindDouble:
			number = value.Double
		default:
			return nil, fmt.Errorf("unit conversion column %q at row %d requires a numeric value", conversion.source, index+1)
		}
		if !conversion.identity {
			number = math.FMA(number-conversion.sourceZero, conversion.ratio, conversion.destZero)
		}
		if math.IsInf(number, 0) || math.IsNaN(number) {
			return nil, fmt.Errorf("unit conversion result for column %q at row %d is not finite", conversion.source, index+1)
		}
		next[conversion.target] = pe.EvalDouble(number).ToJSON()
		out = append(out, next)
	}
	return out, nil
}
