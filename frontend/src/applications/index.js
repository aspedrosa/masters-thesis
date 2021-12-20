import ApplicationsCreate from './ApplicationCreate';
import ApplicationsEdit from './ApplicationEdit';
import ApplicationsList from './ApplicationList';
import ApplicationsShow from './ApplicationShow';

const views = {
    list: ApplicationsList,
    create: ApplicationsCreate,
    edit: ApplicationsEdit,
    show: ApplicationsShow,
};

export default views;