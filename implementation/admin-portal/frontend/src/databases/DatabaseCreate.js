import * as React from "react";
import { Create, SimpleForm, TextInput, ReferenceInput, SelectInput } from 'react-admin';

const DatabasesCreate = (props) => (
    <Create {...props}>
        <SimpleForm>
            <ReferenceInput source="community" reference="communities">
                <SelectInput optionText="name"/>
            </ReferenceInput>
            <TextInput source="name" />
            <TextInput source="database_identifier" />
        </SimpleForm>
    </Create>
);

export default DatabasesCreate;
