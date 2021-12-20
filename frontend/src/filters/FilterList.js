import * as React from "react";
import { List, Datagrid, TextField } from 'react-admin';

const FiltersList = (props) => (
    <List exporter={false} {...props}>
        <Datagrid rowClick="show">
            <TextField source="name" />
            <TextField source="status" />
        </Datagrid>
    </List>
);

export default FiltersList;