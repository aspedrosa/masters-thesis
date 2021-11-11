import * as React from "react";
import COLUMNS from "../columns";
import { Edit, SimpleForm, TextInput, CheckboxGroupInput, ReferenceArrayInput, SelectArrayInput } from 'react-admin';

const FiltersEdit = (props) => {
    const choices = COLUMNS.map(c => ({id: c, name: c}));

    return (
        <Edit {...props}>
            <SimpleForm>
                <ReferenceArrayInput source="communities" reference="communities">
                    <SelectArrayInput optionText="name"></SelectArrayInput>
                </ReferenceArrayInput>
                <TextInput source="name" />
                <TextInput source="filter" />
                <CheckboxGroupInput source="selections" choices={choices}
                helperText="Selecting no checkbox will lead to selecting all columns"
                />
            </SimpleForm>
        </Edit>
    )
}

export default FiltersEdit;
