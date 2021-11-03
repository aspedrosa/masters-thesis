import * as React from "react";
import { TopToolbar, EditButton, Datagrid, Show, SimpleShowLayout, TextField, ReferenceManyField, ArrayField } from 'react-admin';

const CommunitiesShow = (props) => {
    return (
        <Show {...props}>
            <SimpleShowLayout>
                <TextField source="name" />
                <ReferenceManyField reference="databases" target="community_id">
                    <Datagrid>
                    </Datagrid>
                </ReferenceManyField>
                <ReferenceManyField reference="applications" target="community_id">
                    <Datagrid>
                    </Datagrid>
                </ReferenceManyField>
            </SimpleShowLayout>
        </Show>
    );
}

export default CommunitiesShow;