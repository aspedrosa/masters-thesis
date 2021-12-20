import * as React from "react";
import { List, Datagrid, ReferenceField, TextField } from 'react-admin';

const ApplicationsList = (props) => (
    <List exporter={false} {...props}>
        <Datagrid rowClick="show">
            <TextField source="name" />
            <ReferenceField
                label="Community"
                source="community"
                reference="communities"
                link={(record, reference) => `/${reference}/${record.community}/show`}
            >
                <TextField source="name" />
            </ReferenceField>
            <ReferenceField
                label="Filter"
                source="filter"
                reference="filters"
                link={(record, reference) => `/${reference}/${record.filter}/show`}
            >
                <TextField source="name" />
            </ReferenceField>
            <TextField source="status" />
        </Datagrid>
    </List>
);

export default ApplicationsList;