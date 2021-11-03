import * as React from "react";
import { List, Datagrid, ReferenceField, TextField } from 'react-admin';

const CommunitiesList = (props) => (
    <List exporter={false} {...props}>
        <Datagrid rowClick="show">
            <TextField source="name" />
        </Datagrid>
    </List>
);

export default CommunitiesList;