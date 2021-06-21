from flask import Blueprint, current_app, session, request, render_template, flash, redirect, url_for, Markup, abort
import json
from datetime import datetime
import logging

from sres.auth import login_required, is_user_administrator, get_auth_user, log_out, log_in_session, remember_fallback_login
from sres.ldap import find_user as ldap_find_user, find_user_by_username
from sres.users import search_users as db_search_users, get_admins, ADMIN_CATEGORIES, change_admins, get_admins_aggregate, CONFIGURABLE_FIELDS, User
from sres import utils
from sres.studentdata import StudentData
from sres.tables import Table
from sres.columns import Column
from sres.access_logs import add_access_event
from sres.logs import get_feedback_logs

bp = Blueprint('admin', __name__, url_prefix='/admin')

@bp.route('/administrators', methods=['GET', 'POST'])
@login_required
def edit_administrators():
    logging.info("Edit administrators access [{}]".format(get_auth_user()))
    add_access_event(action='view')
    if not is_user_administrator(category='super'):
        flash("Sorry, you are not authorised to complete this action.", "danger")
        return render_template('denied.html')
    admins = get_admins()
    if request.method == 'GET':
        vars = {}
        vars['ADMIN_CATEGORIES'] = ADMIN_CATEGORIES
        vars['nonce'] = utils.generate_nonce()
        for category, config in ADMIN_CATEGORIES.items():
            vars['{}_administrators'.format(category)] = [{'value': u['username'], 'display': u['display_name']} for u in admins[category]]
        return render_template('administrators-edit.html', vars=vars)
    elif request.method == 'POST':
        if request.form.get('action', '') == 'make_list':
            table = Table()
            if request.form.get('make_list_existing_uuid'):
                if table.load(request.form.get('make_list_existing_uuid')):
                    pass
                else:
                    flash("Could not load specified list.", "danger")
            else:
                if table.create():
                    table.config['name'] = "SRES Administrators"
                    table.config['code'] = "SRES_ADMIN"
                    table.config['year'] = datetime.now().year
                    table.config['semester'] = 0
                    table.update()
                else:
                    flash("Could not create list.", "danger")
            if table._id:
                # create a new column
                column = Column(table)
                if column.create(table.config['uuid']):
                    column.config['name'] = "Access level(s) {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    column.config['description'] = "As of " + str(datetime.now())
                    column.update()
                    # get admins
                    admins_aggregate = get_admins_aggregate()
                    # make sure details are all there
                    for username, meta in admins_aggregate.items():
                        if not meta['email'] or meta['display_name'] == username:
                            # user details do not exist properly
                            _users = find_user_by_username(username)
                            if len(_users) == 1 and _users[0]['email']:
                                admins_aggregate[username]['email'] = _users[0]['email']
                                admins_aggregate[username]['given_names'] = _users[0]['given_names']
                                admins_aggregate[username]['preferred_name'] = _users[0]['given_names'].split(' ')[0]
                                admins_aggregate[username]['surname'] = _users[0]['surname']
                    # update 'enrolments'
                    table._update_enrollments(
                        df=[v for k, v in admins_aggregate.items()],
                        mapping={
                            'sid': {'field': 'username'},
                            'email': {'field': 'email'},
                            'preferred_name': {'field': 'preferred_name'},
                            'surname': {'field': 'surname'}
                        },
                        remove_not_present=True,
                        overwrite_details=True
                    )
                    # update data
                    student_data = StudentData(table)
                    for username, admin in admins_aggregate.items():
                        student_data._reset()
                        if student_data.find_student(username):
                            student_data.set_data(
                                column_uuid=column.config['uuid'],
                                data=admin['access_levels'],
                                skip_aggregation=True,
                                skip_auth_checks=True,
                                commit_immediately=True,
                                preloaded_column=column
                            )
                    flash(Markup("OK. <a href=\"{}\">View</a>.".format(url_for('table.view_table', table_uuid=table.config['uuid']))), "success")
                else:
                    flash("Could not create column.", "danger")
            return redirect(url_for('admin.edit_administrators'))
        else:
            # check nonce
            nonce = request.form.get('nonce')
            if not utils.validate_nonce(nonce):
                flash("Unauthorised.", "danger")
                logging.error("Unauthorised attempt to edit administrators [{}]".format(
                    get_auth_user()
                ))
                return render_template('denied.html')
            for category, config in ADMIN_CATEGORIES.items():
                new_usernames = request.form.getlist('authorised_{}_administrators'.format(category))
                existing_usernames = [u['username'] for u in admins[category]]
                added_usernames = list(set(new_usernames) - set(existing_usernames))
                removed_usernames = list(set(existing_usernames) - set(new_usernames))
                result = change_admins(
                    category=category,
                    add_usernames=added_usernames,
                    remove_usernames=removed_usernames
                )
                logging.info("Updated administrators [{}] [{}] [{}]".format(
                    category,
                    str(added_usernames),
                    str(removed_usernames)
                ))
                flash("{} updated. {} added, {} removed".format(category, result['added'], result['removed']), "success")
            return redirect(url_for('admin.edit_administrators'))
    
@bp.route('/view_scheduled_jobs', methods=['GET'])
@login_required
def view_scheduled_jobs():
    add_access_event(action='view')
    if not is_user_administrator(category='super'):
        flash("Sorry, you are not authorised to complete this action.", "danger")
        return render_template('denied.html')
    jobs = []
    for job in current_app.scheduler.get_jobs():
        _job = {
            'id': job.id,
            'id_display': str(job.id).replace('_', '_<wbr>'),
            'name': job.name,
            'args': str(job.args),
            'user': '',
            'kwargs': str(job.kwargs),
            'next_run_time': str(job.next_run_time)
        }
        try:
            if _job['name'] == 'import_handler':
                _job['user'] = job.args[3]
        except:
            # don't worry about it
            pass
        jobs.append(_job)
    return render_template('admin-view-scheduled-jobs.html', jobs=jobs)

@bp.route('/run_scheduled_job', methods=['POST'])
@login_required
def run_scheduled_job():
    add_access_event(action='view')
    if not is_user_administrator(category='super'):
        abort(403)
    id = request.args.get('id')
    if id and current_app.scheduler.get_job(id):
        job = current_app.scheduler.get_job(id)
        job.modify(next_run_time=datetime.now())
        return "requested"
    else:
        abort(404)

@bp.route('/delete_scheduled_job', methods=['POST'])
@login_required
def delete_scheduled_job():
    add_access_event(action='view')
    if not is_user_administrator(category='super'):
        abort(403)
    id = request.args.get('id')
    if id and current_app.scheduler.get_job(id):
        current_app.scheduler.remove_job(id)
        return "requested"
    else:
        abort(404)

@bp.route('/reschedule_scheduled_job', methods=['POST'])
@login_required
def reschedule_scheduled_job():
    add_access_event(action='view')
    if not is_user_administrator(category='super'):
        abort(403)
    id = request.args.get('id')
    trigger = request.args.get('trigger')
    minutes = request.args.get('minutes')
    if id and current_app.scheduler.get_job(id):
        job = current_app.scheduler.get_job(id)
        current_app.scheduler.reschedule_job(id, trigger=trigger, minutes=int(minutes))
        return "requested"
    else:
        abort(404)

@bp.route('/act_as', methods=['GET', 'POST'])
@login_required
def act_as():
    add_access_event(action='view')
    logging.info("Act as access [{}]".format(get_auth_user()))
    # must be superadmin
    if not is_user_administrator(category='super'):
        flash("Sorry, you are not authorised to complete this action.", "danger")
        return render_template('denied.html')
    if request.method == 'GET':
        vars = {}
        vars['nonce'] = utils.generate_nonce()
        return render_template('admin-act-as.html', vars=vars)
    elif request.method == 'POST':
        username = request.form.get('username')
        # check nonce
        if not _validate_nonce(request.form.get('nonce')):
            logging.error("Nonce failed in attempt to act as [{}] by [{}]".format(username,get_auth_user()))
            return render_template('denied.html')
        if not username:
            flash("Misconfiguration.", "danger")
            return render_template('denied.html')
        # can only request to become a non-superadmin username
        if is_user_administrator('super', username):
            flash("Cannot act as a superadministrator.", "danger")
            return render_template('denied.html')
        # continue
        actor_username = get_auth_user()
        logging.info("User [{}] preparing to act as [{}]".format(get_auth_user(), username))
        if log_in_session(username):
            logging.info("User [{}] now acting as [{}]. Check: [{}]".format(actor_username, username, get_auth_user()))
            return redirect(url_for('index.index'))
        else:
            flash("Unexpected error.", "danger")
            return render_template('denied.html')

@bp.route('/users', methods=['GET', 'POST'])
@login_required
def edit_users():
    add_access_event(action='view')
    logging.info("Edit users access [{}]".format(get_auth_user()))
    if not is_user_administrator(category='super'):
        flash("Sorry, you are not authorised to complete this action.", "danger")
        return render_template('denied.html')
    admins = get_admins()
    user = User()
    vars = {}
    vars['CONFIGURABLE_FIELDS'] = CONFIGURABLE_FIELDS
    vars['nonce'] = utils.generate_nonce()
    if request.method == 'GET':
        username = request.args.get('username', '')
        oid = request.args.get('oid', '')
        if oid:
            vars['user_preloaded'] = user.find_user(
                oid=oid,
                add_if_not_exists=False
            )
        elif username:
            vars['user_preloaded'] = user.find_user(
                username=username,
                add_if_not_exists=False
            )
        if (oid or username) and (not vars['user_preloaded']):
            flash('Specified user not found.', 'warning')
        return render_template('users-edit.html', vars=vars, user=user)
    elif request.method == 'POST':
        add_access_event(action=request.form.get('btn_action'))
        if request.form.get('btn_action') == 'find':
            if request.form.get('field_username'):
                user.find_user(
                    username=request.form.get('field_username'),
                    add_if_not_exists=False
                )
            elif request.form.get('field_email'):
                user.find_user(
                    email=request.form.get('field_email'),
                    add_if_not_exists=False
                )
            else:
                flash('Incomplete request.', 'warning')
            if user._id:
                return redirect(url_for('admin.edit_users', username=user.config['username']))
            else:
                flash('Could not find user.', 'warning')
        elif request.form.get('btn_action') == 'edit':
            if _validate_nonce(request.form.get('nonce')):
                if request.form.get('oid') and request.form.get('field_username') and request.form.get('field_email'):
                    if user.find_user(oid=request.form.get('oid'), add_if_not_exists=False):
                        for field in CONFIGURABLE_FIELDS:
                            user.config[field] = request.form.get('field_{}'.format(field)) or user.config[field]
                        if request.form.get('password'):
                            remember_fallback_login(
                                request.form.get('field_username'),
                                request.form.get('password'),
                                user
                            )
                        if user.update():
                            flash('Successfully updated.', 'success')
                        else:
                            flash('Error while updating.', 'warning')
                        return redirect(url_for('admin.edit_users', username=request.form.get('field_username')))
                    else:
                        flash('Could not find user.', 'warning')
                else:
                    flash('Missing essential field(s).', 'warning')
        elif request.form.get('btn_action') == 'create':
            if _validate_nonce(request.form.get('nonce')):
                if request.form.get('field_username') and request.form.get('field_email') and request.form.get('password'):
                    if user.find_user(
                        email=request.form.get('field_email'),
                        username=request.form.get('field_username'),
                        add_if_not_exists=False
                    ):
                        flash('User already exists.', 'warning')
                    else:
                        user.find_user(
                            email=request.form.get('field_email'),
                            username=request.form.get('field_username'),
                            add_if_not_exists=True
                        )
                        for field in CONFIGURABLE_FIELDS:
                            user.config[field] = request.form.get('field_{}'.format(field))
                        remember_fallback_login(
                            request.form.get('field_username'),
                            request.form.get('password'),
                            user
                        )
                        if user.update():
                            flash('Successfully added.', 'success')
                        else:
                            flash('Error while updating.', 'warning')
                        return redirect(url_for('admin.edit_users', username=request.form.get('field_username')))
                else:
                    flash('Missing essential field.', 'warning')
        return render_template('users-edit.html', vars=vars, user=user)

@bp.route('/users/add/bulk', methods=['GET', 'POST'])
@login_required
def add_users_bulk():
    add_access_event(action='view')
    logging.info("Add bulk users access [{}]".format(get_auth_user()))
    if not is_user_administrator(category='super'):
        flash("Sorry, you are not authorised to complete this action.", "danger")
        return render_template('denied.html')
    vars = {}
    vars['CONFIGURABLE_FIELDS'] = CONFIGURABLE_FIELDS
    vars['nonce'] = utils.generate_nonce()
    if request.method == 'GET':
        return render_template('users-add-bulk.html', vars=vars)
    elif request.method == 'POST':
        add_access_event(action='edit')
        nonce = request.form.get('nonce')
        if not utils.validate_nonce(nonce):
            flash("Unauthorised.", "danger")
            logging.error("Unauthorised attempt to edit administrators [{}]".format(
                get_auth_user()
            ))
            return render_template('denied.html')
        if request.form.get('user_info'):
            user_info = request.form.get('user_info').splitlines()
            results = {
                'success': [],
                'failed': []
            }
            for _user in user_info:
                user = User()
                _user_info = _user.split('\t')
                if len(_user_info) == len(CONFIGURABLE_FIELDS) + 1:
                    user.find_user(
                            email=_user_info[ CONFIGURABLE_FIELDS.index('email') ],
                            username=_user_info[ CONFIGURABLE_FIELDS.index('username') ],
                            add_if_not_exists=True
                        )                    
                    # collect user details
                    for i, field in enumerate(CONFIGURABLE_FIELDS):
                        user.config[field] = _user_info[i]
                    # save
                    remember_fallback_login(
                        user.config['username'],
                        _user_info[len(CONFIGURABLE_FIELDS)],
                        user
                    )
                    if user.update():
                        results['success'].append((
                            user.config['username'],
                            'OK'
                        ))
                    else:
                        results['failed'].append((
                            str(_user_info),
                            'Save user error'
                        ))
                else:
                    results['failed'].append((
                        str(_user_info),
                        'Field count error'
                    ))
            flash("Successfully added {} users".format(len(results['success'])), 'success')
            print(results)
            for failed_result in results['failed']:
                flash("Error [{}] adding user with details {}".format(failed_result[1], failed_result[0]))
            return render_template('users-add-bulk.html', vars=vars)
    flash("Bad request", "danger")
    return render_template('denied.html')

@bp.route('/test0', methods=['GET'])
@login_required
def test0():
    if not is_user_administrator(category='super'):
        abort(403)
    import sys
    return str(sys.executable)

@bp.route('/test1', methods=['GET'])
@login_required
def test1():
    if not is_user_administrator(category='super'):
        abort(403)
    import time
    from threading import Thread
    COUNT = 50000000
    def countdown(n):
        while n > 0:
            n -= 1
    t1 = Thread(target=countdown, args=(COUNT//2,))
    t2 = Thread(target=countdown, args=(COUNT//2,))
    start = time.time()
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    end = time.time()
    return 'Time {} - {}, total {}'.format(str(start), str(end), str(end-start))
    
@bp.route('/test2', methods=['GET'])
@login_required
def test2():
    if not is_user_administrator(category='super'):
        abort(403)
    import time
    from multiprocessing import Pool
    COUNT = 50000000
    def countdown(n):
        while n > 0:
            n -= 1
    pool = Pool(processes=2)
    start = time.time()
    r1 = pool.apply_async(countdown, [COUNT//2])
    r2 = pool.apply_async(countdown, [COUNT//2])
    pool.close()
    pool.join()
    end = time.time()
    return 'Time {} - {}, total {}'.format(str(start), str(end), str(end-start))

def _validate_nonce(nonce):
    if utils.validate_nonce(nonce):
        return True
    else:
        flash("Security validation failed.", "danger")
        return False

@bp.route('/db_indexes', methods=['GET'])
@login_required
def manage_db_indexes():
    add_access_event(action='view')
    logging.info("Manage DB indexes access by [{}]".format(get_auth_user()))
    if not is_user_administrator(category='super'):
        flash("Sorry, you are not authorised to complete this action.", "danger")
        return render_template('denied.html')
        
    from sres.db import _check_mongo_indexes, _create_mongo_index, _DB_INDEXES
    
    create_missing_indexes = True if request.args.get('create', None) == '1' else False
    
    if request.method == 'GET':
        ret = []
        for collection, indexes in _DB_INDEXES.items():
            ret.append(f"<hr>COLLECTION: {collection}")
            current_indexes = _check_mongo_indexes(collection)
            for index in indexes:
                index_found = False
                for current_index_name, current_index in current_indexes.items():
                    if current_index['key'] == index['keys']:
                        index_found = True
                        break
                if index_found:
                    ret.append(f"OK {str(current_index)} {str(index)}")
                else:
                    ret.append(f"NOT FOUND {str(index)}")
                    if create_missing_indexes and index['keys'][0][0] != '_id':
                        res = _create_mongo_index(collection, index['keys'], index['unique'])
                        ret.append(f"CREATE RESULT: {str(res)}")
        return '<br><br>'.join(ret) + "<hr>Set URL param <pre>create=1</pre> to force create any indexes not found.<br><br>"
    
@bp.route('/logs/feedback', methods=['GET'])
@login_required
def view_feedback_logs():
    add_access_event(action='view')
    logging.info("View feedback logs access by [{}]".format(get_auth_user()))
    if not is_user_administrator(category='super'):
        flash("Sorry, you are not authorised to complete this action.", "danger")
        return render_template('denied.html')
    res = get_feedback_logs()
    logs = []
    for record in res.get('logs', []):
        log = {
            'source_asset_type': record.get('source_asset_type', ''),
            'source_asset_uuid': record.get('source_asset_uuid', ''),
            'vote': record.get('vote', ''),
            'data': record.get('data', ''),
            'timestamp': record.get('timestamp', '')
        }
        if record.get('source_asset_type', '') == 'filter':
            log['url'] = url_for('filter.view_logs', filter_uuid=record.get('source_asset_uuid', ''))
        elif record.get('source_asset_type', '') == 'portal':
            log['url'] = url_for('portal.view_logs', portal_uuid=record.get('source_asset_uuid', ''))
        logs.append(log)
    vars = {
        'logs': logs
    }
    return render_template('admin-view-feedback-logs.html', vars=vars)
    
