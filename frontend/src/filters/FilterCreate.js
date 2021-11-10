import * as React from "react";
import COLUMNS from "../columns"
import { Create } from 'react-admin';
import ChangeForm from "./changeForm";

const FiltersCreate = (props) => {
    const choices = COLUMNS.map(c => ({id: c, name: c}));

    return (
        <Create {...props}>
            <ChangeForm choices={choices}></ChangeForm>
        </Create>
    );
}

export default FiltersCreate;
