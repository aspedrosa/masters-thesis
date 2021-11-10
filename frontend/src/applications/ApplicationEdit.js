import * as React from "react";
import { Edit } from 'react-admin';
import ChangeForm from './changeForm';

const ApplicationsEdit = (props) => (
    <Edit {...props}>
        <ChangeForm></ChangeForm>
    </Edit>
);

export default ApplicationsEdit;
