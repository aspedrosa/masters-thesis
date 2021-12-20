import * as React from "react";
import COLUMNS from "../columns"
import { Create, SimpleForm, TextInput, CheckboxGroupInput, ReferenceArrayInput, SelectArrayInput } from 'react-admin';

const FiltersCreate = (props) => {
    const choices = COLUMNS.map(c => ({id: c, name: c}));

    return (
        <Create {...props}>
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
        </Create>
    );
}

export default FiltersCreate;
