import { Server, Config, Context, ResponseFunction, Permission } from 'hive-server';
import * as Redis from "redis";
import * as nanomsg from 'nanomsg';
import * as msgpack from 'msgpack-lite';
import * as http from 'http';
import * as bunyan from 'bunyan';
import * as uuid from 'node-uuid';

let log = bunyan.createLogger({
  name: 'vehicle-server',
  streams: [
    {
      level: 'info',
      path: '/var/log/server-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/server-error.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1w',   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let redis = Redis.createClient(6379, "redis"); // port, host
let list_key = "vehicle-model";
let entity_key = "vehicle-model-entities";
let vehicle_key = "vehicle";
let vehicle_entities = "vehicle_entities";

let config: Config = {
  svraddr: 'tcp://0.0.0.0:4040',
  msgaddr: 'ipc:///tmp/queue.ipc'
};

let svc = new Server(config);



let permissions: Permission[] = [['mobile', true], ['admin', true]];


svc.call('getVehicleInfo', permissions, (ctx: Context, rep: ResponseFunction, vid:string) => {
  log.info('getVehicleInfos %j', ctx);
  redis.hget(vehicle_entities, vid, function (err, result) {
    if (err) {
      rep([]);
    } else {
      rep(result);
    }
  });
});

svc.call('getVehicleInfos', permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info('getVehicleInfos %j', ctx);
  redis.lrange(vehicle_key, 0 -1, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2objects(vehicle_entities, result, rep);
    }
  });
});

svc.call('getDriverPids', permissions, (ctx: Context, rep: ResponseFunction, vid) => {
  log.info('getDriverPids %j', ctx);
    redis.hget(vehicle_entities, vid, function(err, result){
      if (err) {
        rep([]);
      } else {
        let vehicle = result;
        let drivers = vehicle.drivers;
        rep(drivers);
      }
    });
});

svc.call('getUserVehicles', permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info('getUser_Vehicles %j', ctx);
    redis.lrange(vehicle_key, 0, -1, function(err, result){
      if (err) {
        rep([]);
      } else {
        let vehicles = result;
        let vid = [];
        for (let vehicle of vehicles){
          if(vehicle.user_id == ctx.uid){
            vid.push(vehicle.id);
          }
        }
        rep(vid);
      }
    });
});


svc.call('setVehicleInfoOnCard', permissions, (ctx: Context, rep: ResponseFunction, name:string,
identity_no:string, phone:string, vehicle_code:string, license_no:string, engine_no:string, 
  register_date:string, average_mileage:string, is_transfer:boolean,last_insurance_company:string, insurance_due_date:string) => {
  log.info('setVehicleInfoOnCard %j', ctx);
  let pid = uuid.v1();
  let vid = uuid.v1();
  let args = [pid, name, identity_no, phone, ctx.uid, vehicle_code, vid,license_no, engine_no, 
  register_date, average_mileage, is_transfer,last_insurance_company, insurance_due_date];
  ctx.msgqueue.send(msgpack.encode({cmd: "setVehicleInfoOnCard", args:args}));
  rep({status: 'okay'});
});

svc.call('setVehicleInfo', permissions, (ctx: Context, rep: ResponseFunction, name:string,  identity_no:string,  
  phone:string, vehicle_code:string, license_no:string, engine_no:string, average_mileage:string, is_transfer:boolean,
  receipt_no:string, receipt_date:string, last_insurance_company:string, insurance_due_date:string) => {
  log.info('setVehicleInfo %j', ctx);
  let pid = uuid.v1();
  let vid = uuid.v1();
  let args = [pid, name, identity_no, phone, ctx.uid, vehicle_code, vid, license_no, engine_no, average_mileage, is_transfer,
   receipt_no, receipt_date, last_insurance_company, insurance_due_date];
  ctx.msgqueue.send(msgpack.encode({cmd: "setVehicleInfo", args: args}));
  rep({status: 'okay'});
});

svc.call('setDriverInfo', permissions, (ctx: Context, rep: ResponseFunction, name:string, identity_no:string, phone:string,is_primary:boolean) => {
  log.info('setDriverInfo %j', ctx);
  let pid = uuid.v1();
  let did = uuid.v1();
  let vid = ctx.uid;
  let args = [pid, did, vid, name,identity_no,phone,is_primary]
  ctx.msgqueue.send(msgpack.encode({cmd: "setDriverInfo", args:args}));
  rep({status: 'okay'});
});

svc.call('changeDriverInfo', permissions, (ctx: Context, rep: ResponseFunction,vid:string, pid:string, name:string, identity_no:string, phone:string) => {
  log.info('changeDriverInfo %j', ctx);
  let args = [vid, pid,name, identity_no,phone];
  ctx.msgqueue.send(msgpack.encode({cmd: "changeDriverInfo", args:args}));
  rep({status: 'okay'});
});

//vehicle_model
svc.call('getVehicleModelsByMake', permissions, (ctx: Context, rep: ResponseFunction, vin: string) => {
  log.info('getVehicleModelsByMake %j', ctx);
  redis.hget(entity_key + vin, function (err, result) {
    if (err) {
      rep([]);
    } else {
      if (result == '') {
        let data = JSON.stringify({
          "channelType": "00",
          "requestCode": "100103",
          "operatorCode": "dev@fengchaohuzhu.com",
          "data": {
            "vinCode": vin
          },
          "dtype": "json",
          "operatorPwd": "2fa392325f0fc080a7131a30a57ad4d3"
        });
        let options = {
          // hostname:'www.baidu.com',
          url: 'http://www.jy-epc.com/api-show/NqAfterMarketDataServlet',
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-json',
            'Content-Length': data.length
          }
        };


        let req = http.request(options, (res) => {
          console.log(`STATUS: ${res.statusCode}`);
          console.log(`HEADERS: ${JSON.stringify(res.headers)}`);
          res.setEncoding('utf8');
          res.on('data', (chunk) => {
            let arg = JSON.parse(chunk);
            let args = arg.result;
            ctx.msgqueue.send(msgpack.encode({ cmd: "getVehicleModelsByMake", args: args }));
            rep(args);
          });
          res.on('end', () => {
          })
        });
        req.on('error', (e) => {
          rep(e + "error!请求数据失败!");
        });

        req.write(data);
        req.end();
      } else {
        ids2objects(list_key, result, rep);
      }
    }
  });
});

svc.call('uploadDriverImages', permissions, (ctx: Context, rep: ResponseFunction, vid:string, driving_frontal_view:string, 
driving_rear_view:string, identity_frontal_view:string, identity_rear_view:string, license_frontal_views:{}) => {
  let args = [vid, driving_frontal_view, driving_rear_view, identity_frontal_view, identity_rear_view, license_frontal_views]
  ctx.msgqueue.send(msgpack.encode({cmd: "uploadDriverImages", args: args}));
});

function ids2objects(key: string, ids: string[], rep: ResponseFunction) {
  let multi = redis.multi();
  for (let id of ids) {
    multi.hget(key, id);
  }
  multi.exec(function(err, replies) {
    rep(replies);
  });
}

log.info('Start server at %s and connect to %s', config.svraddr, config.msgaddr);

svc.run();
