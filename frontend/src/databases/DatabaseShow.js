import * as React from "react";
import { Show, TabbedShowLayout, Datagrid, Tab, TextField, ReferenceField, ReferenceManyField, DateField, NumberField } from 'react-admin';

const DatabasesShow = (props) => {
    return (
        <Show {...props}>
            <TabbedShowLayout>
                <Tab label="Info">
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
                </Tab>
                <Tab label="Uploads">
                    <ReferenceManyField reference="databasesuploads" target="database_id" addLabel={false}>
                        <Datagrid>
                            <DateField showTime={true} source="time"></DateField>
                            <NumberField source="rows"></NumberField>
                        </Datagrid>
                    </ReferenceManyField>
                </Tab>
                <Tab label="Health Checks">
                    <ReferenceManyField reference="agentshealthchecks" target="database_id" addLabel={false}>
                        <Datagrid>
                            <DateField showTime={true} source="time"></DateField>
                            <TextField source="reason"></TextField>
                        </Datagrid>
                    </ReferenceManyField>
                </Tab>
            </TabbedShowLayout>
        </Show>
    );
}

export default DatabasesShow;