import CommunitiesCreate from './CommunityCreate';
import CommunitiesEdit from './CommunityEdit';
import CommunitiesList from './CommunityList';
import CommunitiesShow from './CommunityShow';

const views = {
    list: CommunitiesList,
    create: CommunitiesCreate,
    edit: CommunitiesEdit,
    show: CommunitiesShow,
};

export default views;