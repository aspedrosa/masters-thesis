import * as React from "react";
import Button from '@material-ui/core/Button';
import { TopToolbar, EditButton, Show, TabbedShowLayout, Datagrid, DateField, NumberField, ReferenceManyField, Tab, TextField, ReferenceField, } from 'react-admin';

const start = (record) => {
    fetch(
        `/api/applications/${record.id}/start/`,
        {
            method: "PUT",
            headers: {
                Authorization: "Bearer " + localStorage.getItem("access"),
            },
        }
    )
    .then(() => window.location.reload());
};

const stop = (record) => {
    fetch(
        `/api/applications/${record.id}/stop/`,
        {
            method: "PUT",
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

            { data && data.status === "STOPPED" &&
            <Button color="primary" onClick={() => start(data)}>Start</Button>
            }
            { data && data.status === "ACTIVE" &&
            <Button color="primary" onClick={() => stop(data)}>Stop</Button>
            }
        </TopToolbar>
    );
}

const ApplicationsShow = (props) => {
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
                    <ReferenceField
                        label="Filter"
                        source="filter"
                        reference="filters"
                        link={(record, reference) => `/${reference}/${record.filter}/show`}
                    >
                        <TextField source="name" />
                    </ReferenceField>
                    <TextField source="status" />
                    <TextField source="request_template" />
                </Tab>
                <Tab label="Data Sent">
                    <br/>
                    <span>Note: A response code of "0" means that something wrong when building request's arguments or the HTTP request failed.</span>
                    <br/>
                    <ReferenceManyField reference="applicationdatasent" target="application_id" addLabel={false}>
                        <Datagrid>
                            <DateField showTime={true} source="time"></DateField>
                            <NumberField source="response_code"></NumberField>
                            <TextField source="response_data"></TextField>
                        </Datagrid>
                    </ReferenceManyField>
                </Tab>
            </TabbedShowLayout>
        </Show>
    );
}

export default ApplicationsShow;