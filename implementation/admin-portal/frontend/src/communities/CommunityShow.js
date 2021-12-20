import * as React from "react";
import { Show, SimpleShowLayout, TextField, ReferenceManyField, SingleFieldList, ChipField } from 'react-admin';


const CommunitiesShow = (props) => {
    return (
        <Show {...props}>
            <SimpleShowLayout>
                <TextField source="name" />
                <ReferenceManyField reference="databases" target="community" label="Databases">
                    <SingleFieldList linkType={true}>
                        <ChipField source="name" />
                    </SingleFieldList>
                </ReferenceManyField>
                <ReferenceManyField reference="filters" target="communities" label="Filters">
                    <SingleFieldList linkType={true}>
                        <ChipField source="name" />
                    </SingleFieldList>
                </ReferenceManyField>
                <ReferenceManyField reference="applications" target="community" label="Applications">
                    <SingleFieldList linkType={true}>
                        <ChipField source="name" />
                    </SingleFieldList>
                </ReferenceManyField>
            </SimpleShowLayout>
        </Show>
    );
}

export default CommunitiesShow;