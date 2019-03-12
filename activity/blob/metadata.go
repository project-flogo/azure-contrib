package blob

import (
	"github.com/project-flogo/core/data/coerce"
)

type Settings struct {
	AZURE_STORAGE_ACCOUNT    string `md:"azure_storage_account,required"`
	AZURE_STORAGE_ACCESS_KEY string `md:"azure_storage_access_key,required"`
	Method                   string `md:"method,required"`
	ContainerName            string `md:"container_name,required"`
}
type Input struct {
	File string `md:"file"`
	Data string `md:"data"`
}

type Output struct {
	Result map[string]interface{} `md:"result"`
}

func (o *Input) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"file": o.File,
		"data": o.Data,
	}
}

func (o *Input) FromMap(values map[string]interface{}) error {

	var err error
	o.File, err = coerce.ToString(values["file"])
	if err != nil {
		return err
	}
	o.Data, err = coerce.ToString(values["data"])
	if err != nil {
		return err
	}

	return nil
}

func (r *Output) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"result": r.Result,
	}
}

func (r *Output) FromMap(values map[string]interface{}) error {

	r.Result, _ = coerce.ToObject(values["result"])

	return nil
}
