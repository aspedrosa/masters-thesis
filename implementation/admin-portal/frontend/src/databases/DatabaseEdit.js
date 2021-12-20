import * as React from "react";
import { Edit, SimpleForm, TextInput } from 'react-admin';

const DatabasesEdit = (props) => (
    <Edit {...props}>
        <SimpleForm>
            <TextInput source="name" />
        </SimpleForm>
    </Edit>
)

export default DatabasesEdit;
