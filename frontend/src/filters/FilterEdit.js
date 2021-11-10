import * as React from "react";
import COLUMNS from "../columns";
import { Edit } from 'react-admin';
import ChangeForm from "./changeForm";

const FiltersEdit = (props) => {
    const choices = COLUMNS.map(c => ({id: c, name: c}));

    return (
        <Edit {...props}>
            <ChangeForm choices={choices}></ChangeForm>
        </Edit>
    )
}

export default FiltersEdit;
