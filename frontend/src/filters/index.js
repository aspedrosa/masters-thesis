import FiltersCreate from './FilterCreate';
import FiltersEdit from './FilterEdit';
import FiltersList from './FilterList';
import FiltersShow from './FilterShow';

const views = {
    list: FiltersList,
    create: FiltersCreate,
    edit: FiltersEdit,
    show: FiltersShow,
};

export default views;