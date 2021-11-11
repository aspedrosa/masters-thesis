import * as React from "react";
import Button from '@material-ui/core/Button';
import { TopToolbar, EditButton, Show, TabbedShowLayout, Datagrid, Tab, TextField, ReferenceField, ReferenceManyField, DateField, NumberField } from 'react-admin';

const request_health_check = (record) => {
    fetch(
        `/api/databases/${record.id}/request_health_check/`,
        {
            method: "POST",
            headers: {
                Authorization: "Bearer " + localStorage.getItem("access"),
            },
        }
    )
    .then(() => window.location.reload());
};

const PostShowActions = ({ basePath, data, resource }) => {
    return (
        <TopToolbar>
            <EditButton basePath={basePath} record={data} />
            <Button color="primary" onClick={() => request_health_check(data)}>Request Health Check</Button>
        </TopToolbar>
    );
}

const DatabasesShow = (props) => {
    return (
        <Show actions={<PostShowActions/>} {...props}>
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
                    <TextField source="database_identifier" />
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