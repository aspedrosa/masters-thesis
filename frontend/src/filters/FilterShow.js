import * as React from "react";
import Button from '@material-ui/core/Button';
import { ReferenceManyField, TopToolbar, EditButton, Show, SimpleShowLayout, TextField, ArrayField, SingleFieldList, ChipField } from 'react-admin';

export const StringToLabelObject = ({ record, children, ...rest }) =>
React.cloneElement(children, {
    record: { label: record },
    ...rest,
})

const stop = (record) => {
    fetch(
        `/api/filters/${record.id}/stop/`,
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

            { data && data.status === "ACTIVE" &&
                <Button color="primary" onClick={() => stop(data)}>Stop</Button>
            }
        </TopToolbar>
    );
}

const FiltersShow = (props) => {
    return (
        <Show actions={<PostShowActions/>} {...props}>
            <SimpleShowLayout>
                <ReferenceManyField reference="communities" target="filters" label="Communities">
                    <SingleFieldList linkType={true}>
                        <ChipField source="name" />
                    </SingleFieldList>
                </ReferenceManyField>
                <TextField source="name" />
                <TextField source="filter" />
                <ArrayField source="selections">
                    <SingleFieldList linkType={false}>
                        <StringToLabelObject>
                            <ChipField source="label" />
                        </StringToLabelObject>
                    </SingleFieldList>
                </ArrayField>
                <TextField source="status" />
            </SimpleShowLayout>
        </Show>
    );
}

export default FiltersShow;