# -*- coding: utf-8 -*-
# Copyright 2014 Powerleader, PLCLOUD
# Author: Joe Lei <jiaomin.lei@powerleader.com.cn>
'''计费表结构'''

import datetime
import uuid
import json

from sqlalchemy.sql import func
from sqlalchemy import desc
import sqlalchemy
from sqlalchemy import and_, or_

from keystone.common import dependency
from keystone.common import sql
from keystone import exception
from keystone.common.sql import migration_helpers
from keystone.openstack.common.db.sqlalchemy import migration
from keystone.openstack.common import timeutils
from keystone.openstack.common import log
from keystone.openstack.common import importutils
from keystone.openstack.common import uuidutils
from keystone.openstack.common.gettextutils import _
from keystone.plcloud.account import Manager as account_manager
from keystone.plcloud.common.utils import normalize_money as _nm
from keystone.plcloud.common import utils
from keystone.plcloud.common import config
from keystone.plcloud import billing
from keystone.plcloud.common.notifier import notify
from keystone.plcloud.account.backends import sql as account_sql

LOG = log.getLogger(__name__)
CONF = config.CONF
REMARKS = {
    'price.update': '价格修改',
    'charge.create': '自动扣费',
    'resource.create': '资源创建',
    'resource.start': '资源启动',
    'resource.update': '资源更新',
    'resource.end': '资源停止',
    'resource.release': '资源释放',
    'error.noack': '等待确认',
    'cdn.flow.recharge': 'CDN流量购买',
}


class Billing(sql.ModelBase):

    '''status
    1 billing OK,
    2 billing Owing
    3 stoped
    4 stoped Owing
    5 released
    11 admin frozon
    12 admin release
    13 admin noack_frozon
    14 admin noack_release
    23 cdn record
    -1 unknown
    '''
    __tablename__ = 'billing'
    id = sql.Column(sql.String(64), primary_key=True)
    res_id = sql.Column(sql.String(128))
    res_name = sql.Column(sql.String(64))
    res_meta = sql.Column(sql.JsonBlob())
    res_type = sql.Column(sql.String(64))
    user_id = sql.Column(sql.String(64))
    tenant_id = sql.Column(sql.String(64))
    region = sql.Column(sql.String(32))
    status = sql.Column(sql.Integer, nullable=False)
    update_at = sql.Column(sql.DateTime)
    created_at = sql.Column(sql.DateTime)
    price = sql.Column(sqlalchemy.BigInteger)
    amount = sql.Column(sqlalchemy.BigInteger)
    billing_type = sql.Column(sql.String(32))

    @classmethod
    def get_ref(cls, session, res_id, tenant_id, region):
        ref = session.query(cls).filter_by(
            res_id=res_id, tenant_id=tenant_id, region=region)
        try:
            return ref.one()
        except sql.NotFound:
            return None

    def get_detail(self, session):
        ref = session.query(BillingDetail).filter_by(
            billing_id=self.id).order_by(desc(BillingDetail.end_at)).limit(1)
        try:
            return ref.one()
        except sql.NotFound:
            return None

    @property
    def is_billing(self):
        return self.status in [1, 2]

    def to_dict(self, out=False):
        if out:
            if self.res_type == 'cdn':
                unit_price = _nm(self.amount)
            else:
                unit_price = _nm(self.price * 3600)
            data = {'res_id': self.id,
                    'res_name': self.res_name,
                    'res_type': self.res_type,
                    'is_billing': self.is_billing,
                    'unit_price': unit_price,
                    'region': self.region,
                    'amount': _nm(self.amount)}
            return data

        data = {'id': self.id,
                'res_id': self.res_id,
                'res_name': self.res_name,
                'res_meta': self.res_meta,
                'res_type': self.res_type,
                'user_id': self.user_id,
                'tenant_id': self.tenant_id,
                'region': self.region,
                'status': self.status,
                'update_at': self.update_at,
                'created_at': self.created_at,
                'price': self.price,
                'amount': self.amount}
        return data


class BillingDetail(sql.ModelBase):
    __tablename__ = 'billing_detail'
    id = sql.Column(sql.String(64), primary_key=True)
    billing_id = sql.Column(sql.String(64), nullable=False)
    res_meta = sql.Column(sql.JsonBlob())
    price = sql.Column(sqlalchemy.BigInteger)
    amount = sql.Column(sqlalchemy.BigInteger)
    start_at = sql.Column(sql.DateTime)
    end_at = sql.Column(sql.DateTime)
    start_message_id = sql.Column(sql.String(64))
    end_message_id = sql.Column(sql.String(64))
    remarks = sql.Column(sql.String(64))

    @property
    def end_billing(self):
        return uuidutils.is_uuid_like(self.end_message_id)

    def to_dict(self, billing_ref):
        if billing_ref.res_type == 'cdn':
            unit_price = self.amount
        else:
            unit_price = self.price * 3600
        data = {'id': self.id,
                'billing_id': self.billing_id,
                'res_id': billing_ref.res_id,
                'res_name': billing_ref.res_name,
                'res_type': billing_ref.res_type,
                'unit_price': _nm(unit_price),
                'start_at': self.start_at,
                'end_at': self.end_at,
                'region': billing_ref.region,
                'amount': _nm(self.amount),
                'is_billing': billing_ref.is_billing,
                'res_meta': self.res_meta,
                'cost_time': getattr(self, 'cost_time', 0),
                'remarks': REMARKS.get(self.remarks)}
        return data


class BillingRecords(sql.ModelBase):
    __tablename__ = 'billing_records'
    id = sql.Column(sql.String(64), primary_key=True)
    detail_id = sql.Column(sql.String(64), nullable=False)
    start_at = sql.Column(sql.DateTime)
    end_at = sql.Column(sql.DateTime)
    price = sql.Column(sqlalchemy.BigInteger)
    amount = sql.Column(sqlalchemy.BigInteger)
    remarks = sql.Column(sql.String(64))


class BillingEvents(sql.ModelBase, sql.DictBase):
    __tablename__ = 'billing_events'
    attributes = ['message_id', 'res_id', 'res_name', 'res_meta', 'res_type',
                  'event_type', 'timestamp', 'region', 'user_id',
                  'tenant_id']
    message_id = sql.Column(sql.String(64), primary_key=True)
    res_id = sql.Column(sql.String(128), primary_key=True)
    res_name = sql.Column(sql.String(64))
    res_meta = sql.Column(sql.JsonBlob())
    res_type = sql.Column(sql.String(32))
    event_type = sql.Column(sql.String(64))
    timestamp = sql.Column(sql.DateTime)
    region = sql.Column(sql.String(32))
    user_id = sql.Column(sql.String(64))
    tenant_id = sql.Column(sql.String(64))
    extra = sql.Column(sql.JsonBlob())


class BasePrice(sql.ModelBase):
    __tablename__ = 'base_price'
    id = sql.Column(sql.String(64), primary_key=True)
    product_type = sql.Column(sql.String(32), nullable=False)
    product_id = sql.Column(sql.String(128), nullable=False)
    region = sql.Column(sql.String(32), nullable=False)
    enabled = sql.Column(sql.Boolean)
    updated = sql.Column(sql.Boolean, nullable=False, default=True)
    updated_at = sql.Column(sql.DateTime)
    last_update = sql.Column(sql.DateTime)
    start = sql.Column(sql.Integer, nullable=False)
    end = sql.Column(sql.Integer, nullable=False)
    kwargs = sql.Column(sql.JsonBlob(), nullable=False)
    formula = sql.Column(sql.String(256), nullable=False)
    comment = sql.Column(sql.String(512))
    billing_type = sql.Column(sql.String(32))

    def to_dict(self):
        return {'product_type': self.product_type,
                'product_id': self.product_id,
                'region': self.region,
                'start': self.start,
                'end': self.end,
                'kwargs': self.kwargs,
                'formula': self.formula}


@dependency.requires('assignment_api', 'account_api')
class BillingDriver(billing.Driver):

    # Internal interface to manage the database
    def db_sync(self, version=None):
        migration.db_sync(
            sql.get_engine(), migration_helpers.find_migrate_repo(),
            version=version)

    def get_price(self, region, **kwargs):
        session = sql.get_session()
        return self._get_price(session, region, 'hour', **kwargs)

    def create_price(self, region, **kwargs):
        pass

    def update_price(self, region, **kwargs):
        pass

    def disable_price(self, region, price_id):
        pass

    def enable_price(self, region, price_id):
        pass

    def update(self, billing_id, status):
        with sql.transaction() as session:
            ref = session.query(Billing).get(billing_id)
            if not ref:
                msg = _("Could not find billing, %s." % billing_id)
                raise exception.NotFound(message=msg)
            self._update_status(ref, status)
            return ref.to_dict()

    def _get_price(self, session, region, billing_type, **kwargs):
        query = session.query(BasePrice).filter_by(region=region, billing_type=billing_type, enabled=True)
        base = query.filter(BasePrice.product_id.in_(kwargs.keys())).all()
        total_price = 0
        for k, v in kwargs.items():
            try:
                v = int(v)
            except ValueError:
                continue
            f = lambda x: all((x.product_id == k,
                               v > x.start,
                               v <= x.end))
            _base = filter(f, base)
            if not _base:
                continue
            _base = _base[0]
            formula = importutils.import_class(_base.formula)(**_base.kwargs)
            total_price += formula.price(v)
        return total_price

    def _log_billing_event(self, session, event):
        event_ref = BillingEvents.from_dict(event)
        session.add(event_ref)
        return event_ref

    def log_billing_event(self, event):
        with sql.transaction() as session:
            event_ref = self._log_billing_event(session, event)
        return event_ref

    def _get_project(self, session, payload):
        try:
            return self.account_api._get_project(
                session, payload['tenant_id'])
        except exception.ProjectNotFound:
            notify('destory', 'plcloud.billing', **payload)
        return

    def _cmp_balance(self, session, ref, price):
        if isinstance(ref, Billing):
            payload = {'user_id': ref.user_id,
                       'tenant_id': ref.tenant_id,
                       'res_id': ref.res_id,
                       'res_type': ref.res_type,
                       'region': ref.region}
        elif isinstance(ref, dict):
            payload = {'user_id': ref['user_id'],
                       'tenant_id': ref['tenant_id'],
                       'res_id': ref['res_id'],
                       'res_type': ref['res_type'],
                       'region': ref['region']}
        else:
            msg = _('ref %s not Billing or event' % ref)
            raise exception.ValidationError(message=msg)
        if payload['tenant_id'] in CONF.plcloud.billing_whitelist:
            return True
        try:
            account_api = getattr(self, 'account_api')
            if not account_api:
                self.account_api = account_manager()
            project_ref = account_api._get_project(
                session, payload['tenant_id'])
        except exception.ProjectNotFound:
            notify('destory', 'plcloud.billing', **payload)
            return False
        amount = price * CONF.plcloud.charge_seconds
        if project_ref.project_ext.account_balance < amount:
            notify('stop', 'plcloud.billing', **payload)
            return False
        return True

    def _update_status(self, billing_ref, status):
        if billing_ref.status != status:
            billing_ref.status = status
            billing_ref.update_at = datetime.datetime.utcnow()
        return billing_ref

    def _start_detail(self, session, billing_ref, start_at,
                    message_id,billing_type,
                    remarks='resource.start'):
        if billing_type == 'month':
            amount = billing_ref.price * CONF.plcloud.charge_seconds
            end_at = start_at + datetime.timedelta(
                days=CONF.plcloud.charge_hours / 24)
        else:
            amount = billing_ref.price * CONF.plcloud.charge_seconds
            end_at = start_at + datetime.timedelta(
                seconds=CONF.plcloud.charge_seconds)

        detail_ref = BillingDetail(
            id=str(uuid.uuid4()),
            billing_id=billing_ref.id,
            res_meta=billing_ref.res_meta,
            price=billing_ref.price,
            amount=amount,
            start_at=start_at,
            end_at=end_at,
            start_message_id=message_id,
            end_message_id=None,
            remarks=remarks)
        session.add(detail_ref)

        record_ref = BillingRecords(
            id=str(uuid.uuid4()),
            detail_id=detail_ref.id,
            start_at=start_at,
            end_at=end_at,
            price=detail_ref.price,
            amount=amount,
            remarks=remarks)
        session.add(record_ref)

        return amount

    def _end_detail(self, session, billing_ref, end_at, message_id,
                    remarks='resource.end'):
        detail_ref = billing_ref.get_detail(session)
        if detail_ref and not detail_ref.end_billing:
            start_at = detail_ref.end_at
            amount = billing_ref.price * int(timeutils.delta_seconds(
                start_at, end_at))
            detail_ref.amount += amount
            detail_ref.end_at = end_at
            detail_ref.end_message_id = message_id
            detail_ref.remarks = remarks
        else:
            start_at = None
            amount = 0
            remarks = 'error.noack'
            detail_ref = BillingDetail(
                id=str(uuid.uuid4()),
                billing_id=billing_ref.id,
                res_meta=billing_ref.res_meta,
                price=billing_ref.price,
                amount=amount,
                start_at=None,
                end_at=end_at,
                start_message_id=None,
                end_message_id=message_id,
                remarks=remarks)
            session.add(detail_ref)

        record_ref = BillingRecords(
            id=str(uuid.uuid4()),
            detail_id=detail_ref.id,
            start_at=start_at,
            end_at=end_at,
            price=detail_ref.price,
            amount=amount,
            remarks=remarks)
        session.add(record_ref)
        return amount

    def create_billing_type(self, billing_type, res_id):
        with sql.transaction() as session:
            print "*****************************"
            print "res id is: %s" % res_id
            print "*****************************"
            billing_ref = Billing(
                id=str(uuid.uuid4()),
                res_id=res_id,
                price=0,
                amount=0,
                status=1,
                billing_type=billing_type)
            session.add(billing_ref)


    def update_billing_type(self, region, billing_type, res_id):
        with sql.transaction() as session:
            if billing_type == 'month':
                billing_ref = session.query(Billing).filter_by(res_id=res_id)
                billing_f = session.query(Billing).get(billing_ref.all()[0].id)
                price = self._get_price(
                    session, region, billing_type, **billing_f.res_meta)
                billing_f.billing_type = billing_type
                billing_f.price = price
                billing_detail_ref = session.query(BillingDetail).filter_by(billing_id=billing_f.id)
                billing_detail_f = session.query(BillingDetail).get(billing_detail_ref.all()[0].id)
                amount = price * CONF.plcloud.charge_hours
                billing_detail_f.amount = amount
                billing_detail_f.price = price
                end_at = billing_detail_f.start_at + datetime.timedelta(
                    days=CONF.plcloud.charge_hours / 24)
                billing_detail_f.end_at = end_at
                billing_records_ref = session.query(BillingRecords).filter_by(detail_id=billing_detail_f.id)
                billing_records_f = session.query(BillingRecords).get(billing_records_ref.all()[0].id)
                billing_records_f.end_at = end_at
                billing_records_f.price = price
                billing_records_f.amount = amount
                project_ref = self.account_api._pay(
                    session, billing_f.tenant_id, amount)
            else:
                amount = 0

            if project_ref.project_ext.account_balance < 0:
                _ref = self._update_status(billing_f, 2)
                LOG.warning('Judge %s, release (%s, %s)',
                            billing_f.res_type,
                            billing_f.res_name,
                            billing_f.res_id)
                notify('release', 'plcloud.billing', **_ref.to_dict())
            else:
                _ref = self._update_status(billing_f, 1)
            return amount


    def create_billing(self, event):
        with sql.transaction() as session:
            billing_ref = Billing.get_ref(
                session, event['res_id'], event['tenant_id'], event['region'])
            # create billing res_id must be different
            if billing_ref:
                raise exception.Conflict(
                    type='res_id', details=event['res_id'])

            now = datetime.datetime.utcnow()
            try:
                billing_ref = session.query(Billing).filter_by(res_id=event['res_id'])
            except:
                pass
            if billing_ref:
                billing_ref = session.query(Billing).get(billing_ref.all()[0].id)
                price = self._get_price(
                    session, event['region'], billing_ref.billing_type, **event['res_meta'])
                billing_ref.res_name = event['res_name']
                billing_ref.res_meta = event['res_meta']
                billing_ref.res_type = event['res_type']
                billing_ref.user_id = event['user_id']
                billing_ref.tenant_id = event['tenant_id']
                billing_ref.region = event['region']
                billing_ref.price = price
                billing_ref.amount = 0
                billing_ref.status = 1
                billing_ref.update_at = now
                billing_ref.create_at = event['timestamp']
            else:
                price = self._get_price(
                    session, event['region'], 'hour', **event['res_meta'])
                billing_ref = Billing(
                    id=str(uuid.uuid4()),
                    res_id=event['res_id'],
                    res_name=event['res_name'],
                    res_meta=event['res_meta'],
                    res_type=event['res_type'],
                    user_id=event['user_id'],
                    tenant_id=event['tenant_id'],
                    region=event['region'],
                    price=price,
                    amount=0,
                    status=1,
                    update_at=now,
                    created_at=event['timestamp'],
                    billing_type='hour')
            amount = self._start_detail(
                session, billing_ref, event['timestamp'],
                event['message_id'], 'resource.create')
            billing_ref.amount += amount
            session.add(billing_ref)
            project_ref = self.account_api._pay(
                session, billing_ref.tenant_id, amount)
            if project_ref.project_ext.account_balance < 0:
                _ref = self._update_status(billing_ref, 2)
                LOG.warning('Judge %s, release (%s, %s)',
                            billing_ref.res_type,
                            billing_ref.res_name,
                            billing_ref.res_id)
                notify('release', 'plcloud.billing', **_ref.to_dict())
            else:
                _ref = self._update_status(billing_ref, 1)
            return amount

    def delete_billing(self, event):
        import pprint
        print "*******************************************"
        print "delete billing"
        pprint.pprint(event)
        print "*******************************************"
        with sql.transaction() as session:
            billing_ref = Billing.get_ref(
                session, event['res_id'], event['tenant_id'], event['region'])
            now = datetime.datetime.utcnow()

            if billing_ref:
                amount = self._end_detail(
                    session, billing_ref, event['timestamp'],
                    event['message_id'], 'resource.release')
                billing_ref.amount += amount
                billing_ref.status = 5
                billing_ref.update_at = now
            else:
                price = self._get_price(
                    session, event['region'], **event['res_meta'])
                billing_ref = Billing(
                    id=str(uuid.uuid4()),
                    res_id=event['res_id'],
                    res_name=event['res_name'],
                    res_meta=event['res_meta'],
                    res_type=event['res_type'],
                    user_id=event['user_id'],
                    tenant_id=event['tenant_id'],
                    region=event['region'],
                    price=price,
                    amount=0,
                    status=5,
                    update_at=now,
                    created_at=event['timestamp'])
                amount = self._end_detail(
                    session, billing_ref, event['timestamp'],
                    event['message_id'], 'resource.release')
                billing_ref.amount += amount
                # only one commit
                session.add(billing_ref)
            self.account_api._pay(
                session, billing_ref.tenant_id, amount)
            return amount

    def start_billing(self, event):
        import pprint
        print "*******************************************"
        print "start billing"
        pprint.pprint(event)
        print "*******************************************"
        with sql.transaction() as session:
            billing_ref = Billing.get_ref(
                session, event['res_id'], event['tenant_id'], event['region'])
            # count price everytime
            price = self._get_price(
                session, event['region'], **event['res_meta'])

            if billing_ref:
                billing_ref.price = price
                amount = self._start_detail(
                    session, billing_ref, event['timestamp'],
                    event['message_id'])
                billing_ref.amount += amount
            else:
                now = datetime.datetime.utcnow()
                billing_ref = Billing(
                    id=str(uuid.uuid4()),
                    res_id=event['res_id'],
                    res_name=event['res_name'],
                    res_meta=event['res_meta'],
                    res_type=event['res_type'],
                    user_id=event['user_id'],
                    tenant_id=event['tenant_id'],
                    region=event['region'],
                    price=price,
                    amount=0,
                    status=1,
                    update_at=now,
                    created_at=event['timestamp'])
                amount = self._start_detail(
                    session, billing_ref, event['timestamp'],
                    event['message_id'], 'resource.create')
                billing_ref.amount += amount
                # only one commit
                session.add(billing_ref)
            project_ref = self.account_api._pay(
                session, billing_ref.tenant_id, amount)
            if project_ref.project_ext.account_balance < 0:
                _ref = self._update_status(billing_ref, 2)
                LOG.info('Judge %s, stop (%s, %s)',
                         billing_ref.res_type,
                         billing_ref.res_name,
                         billing_ref.res_id)
                notify('stop', 'plcloud.billing', **_ref.to_dict())
            else:
                _ref = self._update_status(billing_ref, 1)
            return amount

    def end_billing(self, event):
        import pprint
        print "*******************************************"
        print "end billing"
        pprint.pprint(event)
        print "*******************************************"
        with sql.transaction() as session:
            billing_ref = Billing.get_ref(
                session, event['res_id'], event['tenant_id'], event['region'])

            if billing_ref:
                amount = self._end_detail(
                    session, billing_ref, event['timestamp'],
                    event['message_id'])
                billing_ref.status = 3
                billing_ref.amount += amount
            else:
                price = self._get_price(
                    session, event['region'], **event['res_meta'])
                now = datetime.datetime.utcnow()
                billing_ref = Billing(
                    id=str(uuid.uuid4()),
                    res_id=event['res_id'],
                    res_name=event['res_name'],
                    res_meta=event['res_meta'],
                    res_type=event['res_type'],
                    user_id=event['user_id'],
                    tenant_id=event['tenant_id'],
                    region=event['region'],
                    price=price,
                    amount=0,
                    status=3,
                    update_at=now,
                    created_at=event['timestamp'])
                amount = self._end_detail(
                    session, billing_ref, event['timestamp'],
                    event['message_id'])
                billing_ref.amount += amount
                # only one commit
                session.add(billing_ref)
            project_ref = self.account_api._pay(
                session, billing_ref.tenant_id, amount)
            if project_ref.project_ext.account_balance < 0:
                self._update_status(billing_ref, 4)
            else:
                self._update_status(billing_ref, 3)
            return amount

    def update_billing(self, event):
        with sql.transaction() as session:
            billing_ref = Billing.get_ref(
                session, event['res_id'], event['tenant_id'], event['region'])
            price = self._get_price(
                session, event['region'], **event['res_meta'])

            if billing_ref:
                amount = self._end_detail(
                    session, billing_ref, event['timestamp'],
                    event['message_id'], 'resource.update')
                billing_ref.res_meta = event['res_meta']
                billing_ref.price = price

                amount += self._start_detail(
                    session, billing_ref, event['timestamp'],
                    event['message_id'], 'resource.update')
                billing_ref.amount += amount
            else:
                now = datetime.datetime.utcnow()
                billing_ref = Billing(
                    id=str(uuid.uuid4()),
                    res_id=event['res_id'],
                    res_name=event['res_name'],
                    res_meta=event['res_meta'],
                    res_type=event['res_type'],
                    user_id=event['user_id'],
                    tenant_id=event['tenant_id'],
                    region=event['region'],
                    price=price,
                    amount=0,
                    status=1,
                    update_at=now,
                    created_at=event['timestamp'])
                amount = self._start_detail(
                    session, billing_ref, event['timestamp'],
                    event['message_id'], 'resource.update')
                billing_ref.amount += amount
                # only one commit
                session.add(billing_ref)
            project_ref = self.account_api._pay(
                session, billing_ref.tenant_id, amount)
            if project_ref.project_ext.account_balance < 0:
                _ref = self._update_status(billing_ref, 2)
                LOG.info('Judge %s, stop (%s, %s)',
                         billing_ref.res_type,
                         billing_ref.res_name,
                         billing_ref.res_id)
                notify('stop', 'plcloud.billing', **_ref.to_dict())
            else:
                self._update_status(billing_ref, 1)
            return amount

    def list_billing(self, tenant_id, region=None, res_type=None,
                     start_at=None, end_at=None, is_billing=None, admin=False):
        session = sql.get_session()
        # only start_at is true and end_at is true
        if start_at and end_at:
            # add by kevin chou 2015/3/18
            # query condition: region is true and res_type is true
            if region and res_type:
                query = session.query(Billing, BillingDetail).filter(
                    and_(BillingDetail.billing_id == Billing.id,
                         Billing.tenant_id == tenant_id,
                         Billing.region == region,
                         Billing.res_type == res_type,
                         BillingDetail.amount != 0))
            # query condition: region is true and res_type is false
            elif region and res_type is None:
                query = session.query(Billing, BillingDetail).filter(
                    and_(BillingDetail.billing_id == Billing.id,
                         Billing.tenant_id == tenant_id,
                         Billing.region == region,
                         BillingDetail.amount != 0))
            # query condition: res_type is true and region is true
            elif res_type and region is None:
                query = session.query(Billing, BillingDetail).filter(
                    and_(BillingDetail.billing_id == Billing.id,
                         Billing.tenant_id == tenant_id,
                         Billing.res_type == res_type,
                         BillingDetail.amount != 0))
            # query condition: res_type is false and region is false
            else:
                query = session.query(Billing, BillingDetail).filter(
                    and_(BillingDetail.billing_id == Billing.id,
                         Billing.tenant_id == tenant_id,
                         BillingDetail.amount != 0))
            # condition: query is true and start_at is true
            # and end_at is true
            if query and start_at and end_at:
                # reset query date time for supportting same day query
                if start_at == end_at:
                    fmt = '%Y-%m-%d %H:%M:%S'
                    start_str = start_at.strftime(fmt)
                    end_str = '%s 23:59:59' % (start_str.split()[0],)
                    start_at = timeutils.parse_strtime(start_str, fmt)
                    end_at = timeutils.parse_strtime(end_str, fmt)
                # reset query date time for supportting unsame day query
                else:
                    fmt = '%Y-%m-%d %H:%M:%S'
                    start_str = start_at.strftime(fmt)
                    end_str = end_at.strftime(fmt)
                    end_str = '%s 23:59:59' % (end_str.split()[0],)
                    start_at = timeutils.parse_strtime(start_str, fmt)
                    end_at = timeutils.parse_strtime(end_str, fmt)
                # filter data for four condition
                query = query.filter(or_(
                    and_(BillingDetail.start_at <= start_at,
                         BillingDetail.end_at >= start_at,
                         BillingDetail.end_at <= end_at),
                    and_(BillingDetail.start_at >= start_at,
                         BillingDetail.start_at <= end_at,
                         BillingDetail.end_at >= end_at),
                    and_(BillingDetail.start_at >= start_at,
                         BillingDetail.end_at <= end_at),
                    and_(BillingDetail.start_at <= start_at,
                         BillingDetail.end_at >= end_at)))

            if is_billing:
                query = query.filter(Billing.status.in_([1, 2]))
            if not admin:
                query = query.filter_by(tenant_id=tenant_id)
            billdetail_data = []
            # foreach query result to generate new filter data
            for bill_obj, billdetail_obj in query.all():
                # only to GetConsumptionsByQuery action
                # in query page
                # condition: A < B, B < D < C
                if billdetail_obj.start_at <= start_at and\
                        billdetail_obj.end_at >= start_at and\
                        billdetail_obj.end_at <= end_at:
                    # calc cost time
                    billdetail_obj.cost_time = int(timeutils.delta_seconds(
                        start_at, billdetail_obj.end_at))
                    billdetail_obj.amount = billdetail_obj.price * \
                        billdetail_obj.cost_time
                    billdetail_obj.end_at = billdetail_obj.end_at
                    billdetail_obj.start_at = start_at
                # condition: B < A < C, D > C
                elif billdetail_obj.start_at >= start_at and\
                        billdetail_obj.start_at <= end_at and\
                        billdetail_obj.end_at >= end_at:
                    billdetail_obj.cost_time = int(timeutils.delta_seconds(
                        billdetail_obj.start_at, end_at))
                    billdetail_obj.amount = billdetail_obj.price * \
                        billdetail_obj.cost_time
                    billdetail_obj.end_at = end_at
                    billdetail_obj.start_at = billdetail_obj.start_at
                # condition: B < A, D < C
                elif billdetail_obj.start_at >= start_at and\
                        billdetail_obj.end_at <= end_at:
                    billdetail_obj.cost_time = int(timeutils.delta_seconds(
                        billdetail_obj.start_at, billdetail_obj.end_at))
                    billdetail_obj.amount = billdetail_obj.price * \
                        billdetail_obj.cost_time
                    billdetail_obj.end_at = billdetail_obj.end_at
                    billdetail_obj.start_at = billdetail_obj.start_at
                # condition: A < B, D > C
                elif billdetail_obj.start_at <= start_at and\
                        billdetail_obj.end_at >= end_at:
                    billdetail_obj.cost_time = int(timeutils.delta_seconds(
                        start_at, end_at))
                    billdetail_obj.amount = billdetail_obj.price * \
                        billdetail_obj.cost_time
                    billdetail_obj.end_at = end_at
                    billdetail_obj.start_at = start_at
                billdetail_data.append((bill_obj, billdetail_obj,))

            return [i[1].to_dict(i[0]) for i in billdetail_data]
        # only to GetConsumptionsByQuery action
        # in active resoure page,return data is billing object
        elif start_at is None and end_at is None:
            qs = {}
            out = False
            if region:
                qs['region'] = region
            if res_type:
                qs['res_type'] = res_type
            query = session.query(Billing).filter_by(**qs)
            if start_at:
                query = query.filter(Billing.created_at >= start_at)
            if end_at:
                query = query.filter(Billing.created_at <= end_at)
            if is_billing:
                query = query.filter(Billing.status.in_([1, 2]))
            if not admin:
                query = query.filter_by(tenant_id=tenant_id)
                out = True
            return [i.to_dict(out) for i in query.all()]

    def list_billing_summary(self, tenant_id, region=None):
        session = sql.get_session()
        qs = {'tenant_id': tenant_id}
        base = session.query(BasePrice.product_type).filter_by(enabled=True)
        billing = session.query(Billing.res_type,
                                func.sum(Billing.amount),
                                func.count(Billing.res_id)).filter_by(**qs)
        if region:
            base = base.filter_by(region=region)
            billing = billing.filter_by(region=region)
        base = base.distinct('product_type').all()
        billing = billing.group_by('res_type').all()

        billing = dict(map(lambda x: (x[0], (x[1], x[2])), billing))
        summary = [{'res_type': b[0],
                    'amount': _nm(billing.get(b[0], [0, 0])[0]),
                    'count': billing.get(b[0], [0, 0])[1]} for b in base]
        return summary

    def list_billing_details(self, tenant_id, billing_id, start_at=None,
                             end_at=None):
        session = sql.get_session()
        ref = session.query(Billing).get(billing_id)
        if not ref:
            msg = _("Could not find billing, %s." % billing_id)
            raise exception.NotFound(message=msg)
        if ref.tenant_id != tenant_id:
            raise exception.Forbidden()
        query = session.query(BillingDetail).filter_by(billing_id=billing_id)
        if start_at:
            query = query.filter(BillingDetail.start_at >= start_at)
        if end_at:
            query = query.filter(BillingDetail.end_at <= end_at)
        return [i.to_dict(ref) for i in query.all()]

    def get_billing_report(self, tenant_id, region=None, years=1):
        session = sql.get_session()
        qs = {'tenant_id': tenant_id}
        if region:
            qs['region'] = region
        start_at = datetime.datetime.utcnow()
        start_at = start_at.replace(year=start_at.year - years)
        result = []

        billing = session.query(Billing.id).filter_by(**qs).all()
        for b in billing:
            query = session.query(
                BillingDetail.start_at,
                BillingDetail.amount).filter_by(billing_id=b[0]).all()
            query = filter(lambda x: x[1], query)
            if query:
                result += query
        return result

    def get_billing_estimated(self, context, project_id=None, email=None,
                              region=None):
        session = sql.get_session()
        query = session.query(
            func.sum(Billing.price),
            func.count(Billing.id),
            Billing.res_type).filter(
            Billing.status.in_([1, 2]),
            Billing.res_type != 'cdn')
        if 'admin' in context['roles']:
            if project_id:
                query = query.filter_by(tenant_id=project_id)
            elif email:
                project = self.account_api.get_project_by_name(email)
                query = query.filter_by(tenant_id=project.id)
        else:
            # Fix Me, permissions validation
            if project_id:
                query = query.filter_by(tenant_id=project_id)
            elif email:
                raise exception.Forbidden
            else:
                query = query.filter_by(tenant_id=context['project_id'])
        if region:
            query = query.filter_by(region=region)
        query = query.group_by('res_type').all()
        data = [dict(zip(('price', 'count', 'res_type'), i)) for i in query]
        for i in data:
            i['hour'] = _nm(i['price'] * 3600)
            i['day'] = _nm(i['price'] * 3600 * 24)
            i['month'] = _nm(i['price'] * 3600 * 24 * 30)
            i['year'] = _nm(i['price'] * 3600 * 24 * 365)
            del i['price']
        return data

    def _change_price(self, session, billing_ref, end_at, message_id, price,
                      remarks='price.update'):
        detail_ref = billing_ref.get_detail(session)
        if not detail_ref or detail_ref.end_billing:
            LOG.debug('billing %s detail had end', billing_ref.id)
            return 0

        start_at = detail_ref.end_at
        amount = detail_ref.price * int(timeutils.delta_seconds(
            start_at, end_at))
        detail_ref.amount += amount
        detail_ref.end_at = end_at
        detail_ref.end_message_id = message_id
        detail_ref.remarks = remarks

        record_ref = BillingRecords(
            id=str(uuid.uuid4()),
            detail_id=detail_ref.id,
            start_at=start_at,
            end_at=end_at,
            price=detail_ref.price,
            amount=amount,
            remarks=remarks)
        session.add(record_ref)

        # change price
        billing_ref.price = price
        amount += self._start_detail(
            session, billing_ref, end_at, message_id, remarks)
        # billing_ref.amount += amount
        LOG.info('detail %s change price success, amount %s',
                 detail_ref.id, amount)
        return amount

    def change_price(self):
        session = sql.get_session()
        base = session.query(BasePrice).filter_by(
            enabled=True, updated=False).all()
        if not base:
            LOG.debug("not price need update")
            return

        account_api = account_manager()
        query = session.query(Billing).filter(Billing.status.in_([1, 2]))
        event = {'message_id': str(uuid.uuid4()),
                 'res_id': str(utils.CHANGE_PRICE_PLCLOUD),
                 'res_name': 'change_price',
                 'res_meta': [i.to_dict() for i in base],
                 'res_type': 'price',
                 'event_type': 'plcloud.price.update',
                 'timestamp': datetime.datetime.utcnow(),
                 'region': None,
                 'user_id': None,
                 'tenant_id': None}
        self._log_billing_event(session, event)

        for p in base:
            billing = query.filter_by(
                region=p.region, res_type=p.product_type).all()
            with sql.transaction() as p_session:
                for b in billing:
                    price = self._get_price(p_session, p.region, **b.res_meta)
                    if price == b.price:
                        LOG.debug('billing %s price is same,', b.id)
                        continue
                    with sql.transaction() as b_session:
                        # get the transaction session
                        b = b_session.query(Billing).get(b.id)
                        amount = self._change_price(
                            b_session, b, event['timestamp'],
                            event['message_id'], price)
                        if amount:
                            account_api._pay(b_session, b.tenant_id, amount)
                            b.amount += amount
                p = p_session.query(BasePrice).get(p.id)
                p.last_update = p.updated_at
                p.updated_at = datetime.datetime.utcnow()
                p.updated = True
                LOG.debug('base %s success update price %s.%s',
                          p.id, p.product_type, p.product_id)

    def _charge(self, session, billing_ref, remarks='charge.create'):
        detail_ref = billing_ref.get_detail(session)
        if not detail_ref or detail_ref.end_billing:
            LOG.debug('billing %s detail had end', billing_ref.id)
            return 0

        now = datetime.datetime.utcnow()
        delta_seconds = int(timeutils.delta_seconds(detail_ref.end_at, now))
        delta = delta_seconds / CONF.plcloud.charge_seconds + 2
        if delta < 1:
            LOG.debug('detail %s end at %s, not need charge',
                      detail_ref.id, detail_ref.end_at)
            return 0

        start_at = detail_ref.end_at
        delta_seconds = delta * CONF.plcloud.charge_seconds
        end_at = detail_ref.end_at + datetime.timedelta(seconds=delta_seconds)
        amount = billing_ref.price * delta_seconds
        detail_ref.amount += amount
        detail_ref.end_at = end_at
        detail_ref.remarks = remarks

        record_ref = BillingRecords(
            id=str(uuid.uuid4()),
            detail_id=detail_ref.id,
            start_at=start_at,
            end_at=end_at,
            price=detail_ref.price,
            amount=amount,
            remarks=remarks)
        session.add(record_ref)
        LOG.debug('charge detail %s amount %s success',
                  detail_ref.id, amount)
        return amount

    def charge(self):
        session = sql.get_session()
        billings = session.query(Billing).filter(
            Billing.status.in_([1, 2])).all()
        account_api = account_manager()
        for b in billings:
            with sql.transaction() as b_session:
                b = b_session.query(Billing).get(b.id)
                amount = self._charge(b_session, b)
                if amount:
                    project_ref = account_api._pay(
                        b_session, b.tenant_id, amount)
                    if project_ref.project_ext.account_balance < 0:
                        _ref = self._update_status(b, 2)
                        LOG.info('Judge %s, stop (%s, %s)',
                                 b.res_type, b.res_name, b.res_id)
                        notify('stop', 'plcloud.billing', **_ref.to_dict())
                    else:
                        self._update_status(b, 1)
                    b.amount += amount

        LOG.info('Billing charge finished.')

    def release(self):
        session = sql.get_session()
        billings = session.query(Billing).filter(
            Billing.status.in_([2, 4])).all()
        delta = datetime.timedelta(hours=CONF.plcloud.release_hours)
        release_time = datetime.datetime.utcnow() - delta
        for b in billings:
            if b.update_at < release_time:
                LOG.warning('Judge %s, release (%s, %s).',
                            b.res_type, b.res_name, b.res_id)
                notify('release', 'plcloud.billing', **b.to_dict())

        LOG.info('Billing release finished.')

    def summarizing(self):
        from keystone.assignment.backends import sql as assignment_sql
        assignment_api = assignment_sql.Assignment()
        session = sql.get_session()
        billings = session.query(Billing).filter(
            Billing.status.in_([2, 4]),
            Billing.res_type != 'cdn').all()
        if len(billings) == 0:
            return []
        for i in billings:
            if not i.user_id:
                user_ids = assignment_api.list_user_ids_for_project(
                    i.tenant_id)
                if user_ids:
                    setattr(i, 'user_id', user_ids[0])
        users = set([i.user_id for i in billings if i.user_id])
        projects = set([i.tenant_id for i in billings if i.tenant_id])
        users = session.query(account_sql.UserBase).filter(
            account_sql.UserBase.id.in_(users)).all()
        projects = session.query(account_sql.ProjectBase).filter(
            account_sql.ProjectBase.id.in_(projects)).all()
        users = dict((i.id, i) for i in users)
        projects = dict((i.id, i) for i in projects)
        billings = [i.to_dict() for i in billings]
        a = lambda x: x.update({'user': {},
                                'user_ext': {},
                                'tenant': {},
                                'tenant_ext': {}})
        map(a, billings)
        for i in billings:
            user = users.get(i['user_id'])
            if user:
                i['user'] = user.to_dict()
            if user and user.user_ext:
                i['user_ext'] = user.user_ext.to_dict()
            tenant = projects.get(i['tenant_id'])
            if tenant:
                i['tenant'] = tenant.to_dict()
            if tenant and tenant.project_ext:
                i['tenant_ext'] = tenant.project_ext.to_dict()
            i['is_billing'] = i['status'] in [2]
            i['unit_price'] = _nm(i['price'] * 3600)
            i['days'] = _nm(i['price'] * 3600 * 24)
            i['months'] = _nm(i['price'] * 3600 * 24 * 30)
            i['years'] = _nm(i['price'] * 3600 * 24 * 365)
            i['amount'] = _nm(i['amount'])
        return billings

    def estimated(self):
        from keystone.assignment.backends import sql as assignment_sql
        assignment_api = assignment_sql.Assignment()
        session = sql.get_session()
        billings = session.query(
            func.sum(Billing.price),
            Billing.user_id,
            Billing.tenant_id).filter(
            Billing.status.in_([1, 2]),
            Billing.res_type != 'cdn').group_by('tenant_id').all()
        if len(billings) == 0:
            return []
        for i in billings:
            if not i.user_id:
                user_ids = assignment_api.list_user_ids_for_project(
                    i.tenant_id)
                if user_ids:
                    setattr(i, 'user_id', user_ids[0])
        users = set([i.user_id for i in billings if i.user_id])
        projects = set([i.tenant_id for i in billings if i.tenant_id])
        users = session.query(account_sql.UserBase).filter(
            account_sql.UserBase.id.in_(users)).all()
        projects = session.query(account_sql.ProjectBase).filter(
            account_sql.ProjectBase.id.in_(projects)).all()
        users = dict((i.id, i) for i in users)
        projects = dict((i.id, i) for i in projects)
        a = lambda x: billings[0] > 0
        b = lambda x: dict(zip(('price', 'user_id', 'tenant_id'), x))
        c = lambda x: x.update({'user': {},
                                'user_ext': {},
                                'tenant': {},
                                'tenant_ext': {}})
        billings = map(b, filter(a, billings))
        map(c, billings)
        for i in billings:
            user = users.get(i['user_id'])
            if user:
                i['user'] = user.to_dict()
            if user and user.user_ext:
                i['user_ext'] = user.user_ext.to_dict()
            tenant = projects.get(i['tenant_id'])
            if tenant:
                i['tenant'] = tenant.to_dict()
            if tenant and tenant.project_ext:
                i['tenant_ext'] = tenant.project_ext.to_dict()
                account_balance = tenant.project_ext.account_balance
            else:
                account_balance = 0
            i['hours'] = _nm(i['price'] * 3600)
            i['days'] = _nm(i['price'] * 3600 * 24)
            i['months'] = _nm(i['price'] * 3600 * 24 * 30)
            i['years'] = _nm(i['price'] * 3600 * 24 * 365)
            assert i['price'] >= 0, 'price %s should > 0' % i['price']
            if i['price'] == 0:
                continue
            if account_balance > 0:
                i['use_days'] = account_balance / (int(i['price']) * 3600 * 24)
            else:
                i['use_days'] = 0
        release_days = CONF.plcloud.release_hours / 24
        a = lambda x: 'use_days' in x and x['use_days'] < release_days
        return filter(a, billings)

    def summarizing_est(self):
        from keystone.assignment.backends import sql as assignment_sql
        assignment_api = assignment_sql.Assignment()
        session = sql.get_session()
        billings = session.query(Billing).filter(
            Billing.status.in_([1, 2]),
            Billing.res_type != 'cdn').all()
        if len(billings) == 0:
            return []
        for i in billings:
            if not i.user_id:
                user_ids = assignment_api.list_user_ids_for_project(
                    i.tenant_id)
                if user_ids:
                    setattr(i, 'user_id', user_ids[0])
        users = set([i.user_id for i in billings if i.user_id])
        projects = set([i.tenant_id for i in billings if i.tenant_id])
        users = session.query(account_sql.UserBase).filter(
            account_sql.UserBase.id.in_(users)).all()
        projects = session.query(account_sql.ProjectBase).filter(
            account_sql.ProjectBase.id.in_(projects)).all()
        users = dict((i.id, i) for i in users)
        projects = dict((i.id, i) for i in projects)
        billings = [i.to_dict() for i in billings]
        a = lambda x: x.update({'user': {},
                                'user_ext': {},
                                'tenant': {},
                                'tenant_ext': {}})
        map(a, billings)
        for i in billings:
            user = users.get(i['user_id'])
            if user:
                i['user'] = user.to_dict()
            if user and user.user_ext:
                i['user_ext'] = user.user_ext.to_dict()
            tenant = projects.get(i['tenant_id'])
            if tenant:
                i['tenant'] = tenant.to_dict()
            if tenant and tenant.project_ext:
                i['tenant_ext'] = tenant.project_ext.to_dict()
            i['is_billing'] = i['status'] in [2]
            i['unit_price'] = _nm(i['price'] * 3600)
        return billings

    def admin_noack_release(self):
        try:
            from keystone.plcloud.billing.backends import billing_res
        except Exception:
            LOG.error('billing_res not found')
            return 0
        for i in billing_res.resources:
            with sql.transaction() as session:
                refs = session.query(Billing).filter_by(res_id=i[0]).all()
                for ref in refs:
                    LOG.error('%s, %s', ref.res_id, ref.status)
                    self._update_status(ref, 14)

    def admin_noack_recharge(self):
        try:
            from keystone.plcloud.billing.backends import billing_res
        except Exception:
            LOG.error('billing_res not found')
        account_api = account_manager()
        fmt = '%Y-%m-%d %H:%M:%S'
        end_str = '2014-09-28 09:55:00'
        end_at = timeutils.parse_strtime(end_str, fmt)
        result = []
        for i, j in billing_res.resources:
            with sql.transaction() as session:
                refs = session.query(Billing).filter_by(res_id=i).all()
                deleted_at = timeutils.parse_strtime(j, fmt)
                delta = int(timeutils.delta_seconds(deleted_at, end_at))
                if not refs:
                    rt = {'res_id': i,
                          'deleted_at': j,
                          'result': 'NotFound'}
                    result.append(rt)
                for ref in refs:
                    amount = delta * ref.price
                    LOG.info('%s, %s', amount, ref.tenant_id)
                    account_api._pay(session, ref.tenant_id, -amount)
                    rt = {'res_id': i,
                          'deleted_at': j,
                          'end_at': end_str,
                          'delta_seconds': delta,
                          'price': ref.price,
                          'tenant_id': ref.tenant_id,
                          'amount': amount,
                          'result': 'OK'}
                    result.append(rt)
        iso_now = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        with open('/tmp/result-%s.json' % iso_now, 'w') as fh:
            json.dump(result, fh)

    def billing_summary_by_weixin(self, weixin_id, region=None):
        summary = {}
        session = sql.get_session()
        try:
            query = session.query(account_sql.ProjectExt).\
                filter_by(weixin_id=weixin_id)
            ref = query.one()
            qs = {'tenant_id': ref.project_id}
        except sql.NotFound:
            msg = 'weixin_id %s not found' % weixin_id
            raise exception.ProjectNotFound(message=msg)
        base = session.query(BasePrice.product_type).filter_by(enabled=True)
        billing = session.query(Billing.res_type,
                                func.sum(Billing.amount),
                                func.count(Billing.res_id)).filter_by(**qs)
        if region:
            base = base.filter_by(region=region)
            billing = billing.filter_by(region=region)
        base = base.distinct('product_type').all()
        billing = billing.group_by('res_type').all()

        billing = dict(map(lambda x: (x[0], (x[1], x[2])), billing))
        for b in base:
            summary.update({b[0]: _nm(billing.get(b[0], [0, 0])[0]),
                            })
        return summary

    def billing_estimated_by_weixin(self, weixin_id, region=None):
        session = sql.get_session()
        try:
            query = session.query(account_sql.ProjectExt).\
                filter_by(weixin_id=weixin_id)
            ref = query.one()
            tenant_id = ref.project_id
        except sql.NotFound:
            msg = 'weixin_id %s not found' % weixin_id
            raise exception.ProjectNotFound(message=msg)
        query = session.query(
            func.sum(Billing.price),
            func.count(Billing.id),
            Billing.res_type).filter(
            Billing.status.in_([1, 2]),
            Billing.res_type != 'cdn',
            Billing.tenant_id == tenant_id)
        if region:
            query = query.filter_by(region=region)
        query = query.group_by('res_type').all()
        data = [dict(zip(('price', 'count', 'res_type'), i)) for i in query]
        for i in data:
            i['hour'] = _nm(i['price'] * 3600)
            i['day'] = _nm(i['price'] * 3600 * 24)
            i['month'] = _nm(i['price'] * 3600 * 24 * 30)
            i['year'] = _nm(i['price'] * 3600 * 24 * 365)
            del i['price']
        return data
