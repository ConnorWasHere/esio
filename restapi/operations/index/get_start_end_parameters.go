package index

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/swag"

	strfmt "github.com/go-openapi/strfmt"
)

// NewGetStartEndParams creates a new GetStartEndParams object
// with the default values initialized.
func NewGetStartEndParams() GetStartEndParams {
	var ()
	return GetStartEndParams{}
}

// GetStartEndParams contains all the bound params for the get start end operation
// typically these are obtained from a http.Request
//
// swagger:parameters GetStartEnd
type GetStartEndParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request

	/*UTC end time, unix timestamp
	  Required: true
	  In: path
	*/
	End int64
	/*Optional override of the repo pattern, must be URL encoded.
	  In: query
	*/
	RepoPattern *string
	/*Optional override of the index resolution, must be 'day', 'month', or 'year'
	  In: query
	*/
	Resolution *string
	/*UTC start time, unix timestamp
	  Required: true
	  In: path
	*/
	Start int64
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls
func (o *GetStartEndParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error
	o.HTTPRequest = r

	qs := runtime.Values(r.URL.Query())

	rEnd, rhkEnd, _ := route.Params.GetOK("end")
	if err := o.bindEnd(rEnd, rhkEnd, route.Formats); err != nil {
		res = append(res, err)
	}

	qRepoPattern, qhkRepoPattern, _ := qs.GetOK("repo_pattern")
	if err := o.bindRepoPattern(qRepoPattern, qhkRepoPattern, route.Formats); err != nil {
		res = append(res, err)
	}

	qResolution, qhkResolution, _ := qs.GetOK("resolution")
	if err := o.bindResolution(qResolution, qhkResolution, route.Formats); err != nil {
		res = append(res, err)
	}

	rStart, rhkStart, _ := route.Params.GetOK("start")
	if err := o.bindStart(rStart, rhkStart, route.Formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *GetStartEndParams) bindEnd(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	value, err := swag.ConvertInt64(raw)
	if err != nil {
		return errors.InvalidType("end", "path", "int64", raw)
	}
	o.End = value

	return nil
}

func (o *GetStartEndParams) bindRepoPattern(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}
	if raw == "" { // empty values pass all other validations
		return nil
	}

	o.RepoPattern = &raw

	return nil
}

func (o *GetStartEndParams) bindResolution(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}
	if raw == "" { // empty values pass all other validations
		return nil
	}

	o.Resolution = &raw

	return nil
}

func (o *GetStartEndParams) bindStart(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	value, err := swag.ConvertInt64(raw)
	if err != nil {
		return errors.InvalidType("start", "path", "int64", raw)
	}
	o.Start = value

	return nil
}
