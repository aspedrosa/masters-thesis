import { SimpleForm, TextInput, CheckboxGroupInput, ReferenceArrayInput, SelectArrayInput } from 'react-admin';

const ChangeForm = (choices) => (
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
);

export default ChangeForm;