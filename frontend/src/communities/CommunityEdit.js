import * as React from "react";
import { Edit } from 'react-admin';
import ChangeForm from "./changeForm";

const CommunitiesEdit = (props) => (
    <Edit {...props}>
        <ChangeForm></ChangeForm>
    </Edit>
);

export default CommunitiesEdit;
