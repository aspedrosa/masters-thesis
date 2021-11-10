import * as React from "react";
import { Create } from 'react-admin';
import ChangeForm from './changeForm';

const ApplicationsCreate = (props) => (
    <Create {...props}>
        <ChangeForm></ChangeForm>
    </Create>
);

export default ApplicationsCreate;
