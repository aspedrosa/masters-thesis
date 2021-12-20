import * as React from "react";
import { Create, SimpleForm, TextInput } from 'react-admin';

const CommunitiesCreate = (props) => (
    <Create {...props}>
        <SimpleForm>
            <TextInput source="name" />
        </SimpleForm>
    </Create>
);

export default CommunitiesCreate;
