/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
package endpoint

import (
	"context"
	"crypto/tls"
	"go-mysql-transfer/util/logagent"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/olivere/elastic/v7"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/metrics"
	"go-mysql-transfer/model"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/util/logs"
	"go-mysql-transfer/util/stringutil"
)

type Elastic7Endpoint struct {
	first  string
	hosts  []string
	client *elastic.Client

	retryLock sync.Mutex
}

func newElastic7Endpoint() *Elastic7Endpoint {
	hosts := elsHosts(global.Cfg().ElsAddr)
	r := &Elastic7Endpoint{}
	r.hosts = hosts
	r.first = hosts[0]
	return r
}

func (s *Elastic7Endpoint) Connect() error {

	//ESCfg := config.Cfg.Elasticsearch
	//err := es.InitClientWithOptions(es.DefaultClient, ESCfg.Host,
	//	ESCfg.User,
	//	ESCfg.Password,
	//	es.WithScheme("https"))
	//if err != nil {
	//	logger.Error("InitClientWithOptions error", zap.Error(err), zap.String("client", es.DefaultClient))
	//	panic(err)
	//}
	//global.ES = es.GetClient(es.DefaultClient)

	var options []elastic.ClientOptionFunc
	options = append(options, elastic.SetErrorLog(logagent.NewElsLoggerAgent()))
	options = append(options, elastic.SetURL(s.hosts...))
	if global.Cfg().ElsUser != "" && global.Cfg().ElsPassword != "" {
		options = append(options, elastic.SetBasicAuth(global.Cfg().ElsUser, global.Cfg().Password))
	}

	esOptions := getBaseOptions(global.Cfg().ElsUser, global.Cfg().ElsPassword, s.hosts...)

	client, err := elastic.NewClient(esOptions...)
	if err != nil {
		return err
	}

	s.client = client
	return s.indexMapping()

	//err := es.InitClientWithOptions(es.DefaultClient, s.hosts, global.Cfg().ElsUser, global.Cfg().ElsPassword, es.WithScheme("https"))
	//if err != nil {
	//	panic(err)
	//}
	//s.client = es.GetClient(es.DefaultClient).Client
	//
	//return s.indexMapping()
}
func getDefaultClient() *http.Client {
	tr := &http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
	}
	return &http.Client{Transport: tr}
}
func (s *Elastic7Endpoint) indexMapping() error {
	for _, rule := range global.RuleInsList() {
		exists, err := s.client.IndexExists(rule.ElsIndex).Do(context.Background())
		if err != nil {
			return err
		}
		if exists {
			err = s.updateIndexMapping(rule)
		} else {
			err = s.insertIndexMapping(rule)
		}
		if err != nil {
			return err
		}
	}

	return nil
}
func getBaseOptions(username, password string, urls ...string) []elastic.ClientOptionFunc {
	options := make([]elastic.ClientOptionFunc, 0)
	options = append(options, elastic.SetURL(urls...))
	options = append(options, elastic.SetBasicAuth(username, password))
	options = append(options, elastic.SetHealthcheckTimeoutStartup(15*time.Second))
	//开启Sniff，SDK会定期(默认15分钟一次)嗅探集群中全部节点，将全部节点都加入到连接列表中，
	//后续新增的节点也会自动加入到可连接列表，但实际生产中我们可能会设置专门的协调节点，所以默认不开启嗅探
	options = append(options, elastic.SetSniff(false))
	options = append(options, elastic.SetScheme("https"))
	options = append(options, elastic.SetHttpClient(getDefaultClient()))
	options = append(options, elastic.SetHealthcheck(false))
	options = append(options, elastic.SetErrorLog(logagent.NewElsLoggerAgent()))
	return options
}
func (s *Elastic7Endpoint) insertIndexMapping(rule *global.Rule) error {
	var properties map[string]interface{}
	if rule.LuaEnable() {
		properties = buildPropertiesByMappings(rule)
	} else {
		properties = buildPropertiesByRule(rule)
	}

	mapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": properties,
		},
	}
	body := stringutil.ToJsonString(mapping)

	ret, err := s.client.CreateIndex(rule.ElsIndex).Body(body).Do(context.Background())
	if err != nil {
		return err
	}
	if !ret.Acknowledged {
		return errors.Errorf("create index %s err", rule.ElsIndex)
	}

	logs.Infof("create index: %s ,mappings: %s", rule.ElsIndex, body)

	return nil
}

func (s *Elastic7Endpoint) updateIndexMapping(rule *global.Rule) error {

	return nil
	ret, err := s.client.GetMapping().Index(rule.ElsIndex).Do(context.Background())
	if err != nil {
		return err
	}

	if ret[rule.ElsIndex] == nil {
		return nil
	}
	retIndex := ret[rule.ElsIndex].(map[string]interface{})

	if retIndex["mappings"] == nil {
		return nil
	}
	retMaps := retIndex["mappings"].(map[string]interface{})

	if retMaps["properties"] == nil {
		return nil
	}
	retPros := retMaps["properties"].(map[string]interface{})

	var currents map[string]interface{}
	if rule.LuaEnable() {
		currents = buildPropertiesByMappings(rule)
	} else {
		currents = buildPropertiesByRule(rule)
	}

	if len(retPros) < len(currents) {
		properties := make(map[string]interface{})
		mapping := map[string]interface{}{
			"properties": properties,
		}
		for field, current := range currents {
			if _, exist := retPros[field]; !exist {
				properties[field] = current
			}
		}

		doc := stringutil.ToJsonString(mapping)
		ret, err := s.client.PutMapping().Index(rule.ElsIndex).BodyString(doc).Do(context.Background())
		if err != nil {
			return err
		}
		if !ret.Acknowledged {
			return errors.Errorf("update index %s err", rule.ElsIndex)
		}

		logs.Infof("update index: %s ,properties: %s", rule.ElsIndex, doc)
	}

	return nil
}

func (s *Elastic7Endpoint) Ping() error {
	if _, _, err := s.client.Ping(s.first).Do(context.Background()); err == nil {
		return nil
	}

	for _, host := range s.hosts {
		if _, _, err := s.client.Ping(host).Do(context.Background()); err == nil {
			return nil
		}
	}

	return errors.New("ssx")
}

func (s *Elastic7Endpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	bulk := s.client.Bulk()
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		metrics.UpdateActionNum(row.Action, row.RuleKey)

		if rule.LuaEnable() {
			kvm := rowMap(row, rule, true)
			ls, err := luaengine.DoESOps(kvm, row.Action, rule)
			if err != nil {
				log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
				return errors.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
			}
			for _, resp := range ls {
				logs.Infof("action: %s, Index: %s , Id:%s, value: %v", resp.Action, resp.Index, resp.Id, resp.Date)
				s.prepareBulk(resp.Action, resp.Index, resp.Id, resp.Date, bulk)
			}
		} else {
			kvm := rowMap(row, rule, false)
			id := primaryKey(row, rule)
			body := encodeValue(rule, kvm)
			logs.Infof("action: %s, Index: %s , Id:%s, value: %v", row.Action, rule.ElsIndex, id, body)
			s.prepareBulk(row.Action, rule.ElsIndex, stringutil.ToString(id), body, bulk)
		}
	}

	if bulk.NumberOfActions() == 0 {
		return nil
	}

	r, err := bulk.Do(context.Background())
	if err != nil {
		return err
	}

	if len(r.Failed()) > 0 {
		for _, f := range r.Failed() {
			reason := f.Index + " " + f.Type + " " + f.Result
			if f.Error == nil && "not_found" == f.Result {
				return nil
			}

			if f.Error != nil {
				reason = f.Error.Reason
			}
			log.Println(reason)
			return errors.New(reason)
		}
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil
}

func (s *Elastic7Endpoint) Stock(rows []*model.RowRequest) int64 {
	if len(rows) == 0 {
		return 0
	}

	bulk := s.client.Bulk()
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaEnable() {
			kvm := rowMap(row, rule, true)
			ls, err := luaengine.DoESOps(kvm, row.Action, rule)
			if err != nil {
				logs.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
				break
			}
			for _, resp := range ls {
				s.prepareBulk(resp.Action, resp.Index, resp.Id, resp.Date, bulk)
			}
		} else {
			kvm := rowMap(row, rule, false)
			id := primaryKey(row, rule)
			body := encodeValue(rule, kvm)
			s.prepareBulk(row.Action, rule.ElsIndex, stringutil.ToString(id), body, bulk)
		}
	}

	r, err := bulk.Do(context.Background())
	if err != nil {
		logs.Error(errors.ErrorStack(err))
		return 0
	}

	if len(r.Failed()) > 0 {
		for _, f := range r.Failed() {
			logs.Error(f.Error.Reason)
		}
	}

	return int64(len(r.Succeeded()))
}

func (s *Elastic7Endpoint) prepareBulk(action, index, id, doc string, bulk *elastic.BulkService) {
	switch action {
	case canal.InsertAction:
		req := elastic.NewBulkIndexRequest().Index(index).Id(id).Doc(doc)
		bulk.Add(req)
	case canal.UpdateAction:
		//req := elastic.NewBulkUpdateRequest().Index(index).Id(id).Doc(doc)//如果文档不存在会报错，开启服务器之前需要把文档初始化下
		req := elastic.NewBulkIndexRequest().Index(index).Id(id).Doc(doc)
		bulk.Add(req)
	case canal.DeleteAction:
		req := elastic.NewBulkDeleteRequest().Index(index).Id(id)
		bulk.Add(req)
	}

	logs.Infof("index: %s, doc: %s", index, doc)
}

func (s *Elastic7Endpoint) Close() {
	if s.client != nil {
		s.client.Stop()
	}
}
