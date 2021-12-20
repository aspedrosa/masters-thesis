import * as React from "react";
import { Edit, SimpleForm, TextInput, ReferenceInput, SelectInput } from 'react-admin';

const ApplicationsEdit = (props) => (
    <Edit {...props}>
        <SimpleForm>
            <ReferenceInput source="community" reference="communities">
                <SelectInput optionText="name"/>
            </ReferenceInput>
            <TextInput source="name" />
            <ReferenceInput source="filter" reference="filters">
                <SelectInput optionText="name"/>
            </ReferenceInput>
            <TextInput label="Request Template" source="request_template" multiline={true} fullWidth={true} />

            <div style={{ width: "100%" }}>
                This field will be used to generate the parameters to be inserted into
                the <a href="https://docs.python-requests.org/en/latest/api/#requests.request" target="_blank" rel="noreferrer"> request </a> function of the requests python package.
                <br/>
                First, the value of this field will be interpreted as a Jinja Template, where you have access to the variables:
                <ul>
                    <li>community_identifier: string</li>
                    <li>database_identifier: string</li>
                    <li>filtered_data: <a href="https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html" target="_blank" rel="noreferrer">pandas dataframe</a></li>
                </ul>
                <br/>
                Then the result will be executed as Python code, where the variable data_file is available, which is
                a python file pointer, to a file containing the filtered data uploaded by a database.
                <br/>
                It is required that the final result after these two steps is a valid python dictionary.
                </div>
        </SimpleForm>
    </Edit>
);

export default ApplicationsEdit;
