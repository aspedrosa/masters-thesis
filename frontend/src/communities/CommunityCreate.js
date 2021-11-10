import * as React from "react";
import { Create } from 'react-admin';
import ChangeForm from "./changeForm";

const CommunitiesCreate = (props) => (
    <Create {...props}>
        <ChangeForm></ChangeForm>
    </Create>
);

export default CommunitiesCreate;
