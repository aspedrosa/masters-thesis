import * as React from "react";
import { List, Datagrid, TextField, ReferenceField} from 'react-admin';

const DatabasesList = (props) => (
    <List exporter={false} {...props}>
        <Datagrid rowClick="show">
            <ReferenceField
                label="Community"
                source="community"
                reference="communities"
                link={(record, reference) => `/${reference}/${record.community}/show`}
            >
                <TextField source="name" />
            </ReferenceField>
            <TextField source="name" />
            <TextField source="unique_identifier" />
        </Datagrid>
    </List>
);

export default DatabasesList;