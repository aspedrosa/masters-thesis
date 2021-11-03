import * as React from "react";
import { Edit, SimpleForm, TextInput, ArrayInput, SimpleFormIterator } from 'react-admin';

const ApplicationsEdit = (props) => (
    <Edit {...props}>
        <SimpleForm>
            <TextInput source="name" />
            <TextInput label="Request Template" source="request_template" multiline={true} />
        </SimpleForm>
    </Edit>
);

export default ApplicationsEdit;
