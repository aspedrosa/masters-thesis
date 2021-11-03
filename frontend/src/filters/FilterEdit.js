import * as React from "react";
import COLUMNS from "../columns";
import { Edit, SimpleForm, TextInput, CheckboxGroupInput } from 'react-admin';

const FiltersEdit = (props) => {
    const choices = COLUMNS.map(c => ({id: c, name: c}));

    return (
        <Edit {...props}>
            <SimpleForm>
                <TextInput source="name" />
                <TextInput source="filter" />
                <CheckboxGroupInput source="selections" choices={choices} />
            </SimpleForm>
        </Edit>
    )
}

export default FiltersEdit;
