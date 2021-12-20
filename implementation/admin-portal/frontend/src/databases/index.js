import DatabasesCreate from './DatabaseCreate';
import DatabasesEdit from './DatabaseEdit';
import DatabasesList from './DatabaseList';
import DatabasesShow from './DatabaseShow';

const views = {
    list: DatabasesList,
    create: DatabasesCreate,
    edit: DatabasesEdit,
    show: DatabasesShow,
};

export default views;