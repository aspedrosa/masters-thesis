import * as React from "react";
import { Admin, Resource } from 'react-admin';
import drfProvider, { jwtTokenAuthProvider, fetchJsonWithAuthJWTToken } from 'ra-data-django-rest-framework';

import communities from "./communities";
import databases from "./databases";
import filters from "./filters";
import applications from "./applications";


const authProvider = jwtTokenAuthProvider()
const dataProvider = drfProvider("/api", fetchJsonWithAuthJWTToken);

function App() {
  return (
    <Admin dataProvider={dataProvider} authProvider={authProvider}>
        <Resource name="communities" {...communities} />
        <Resource name="databases" {...databases} />
        <Resource name="filters" {...filters} />
        <Resource name="applications" {...applications} />
        <Resource name="agentshealthchecks" />
        <Resource name="databasesuploads" />
        <Resource name="applicationdatasent" />
    </Admin>
  );
}

export default App;
