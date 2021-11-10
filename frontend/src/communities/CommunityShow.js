import * as React from "react";
import { TopToolbar, EditButton, Datagrid, Show, SimpleShowLayout, TextField, ReferenceManyField, ArrayField } from 'react-admin';

const row_click = (id, basePath, record) => {
    window.location.href=`#${basePath}/${id}/show`;
};

const CommunitiesShow = (props) => {
    return (
        <Show {...props}>
            <SimpleShowLayout>
                <TextField source="name" />
                <ReferenceManyField reference="databases" target="community_id" label="Databases">
                    <Datagrid rowClick={row_click}>
                        <TextField source="name"></TextField>
                    </Datagrid>
                </ReferenceManyField>
                <ReferenceManyField reference="filters" target="community_id" label="Filters">
                    <Datagrid rowClick={row_click}>
                        <TextField source="name"></TextField>
                    </Datagrid>
                </ReferenceManyField>
                <ReferenceManyField reference="applications" target="community_id" label="Applications">
                    <Datagrid rowClick={row_click}>
                        <TextField source="name"></TextField>
                    </Datagrid>
                </ReferenceManyField>
            </SimpleShowLayout>
        </Show>
    );
}

export default CommunitiesShow;