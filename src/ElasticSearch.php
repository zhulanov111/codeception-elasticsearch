<?php

namespace Codeception\Module;

use Codeception\Module;
use Codeception\Lib\ModuleContainer;

use Elasticsearch\ClientBuilder;

class Elasticsearch extends Module
{
    public $elasticsearch;

    public function _initialize()
    {
        $clientBuilder = ClientBuilder::create();
        $clientBuilder->setHosts($this->_getConfig('hosts'));
        $this->client = $clientBuilder->build();

        if ($this->_getConfig('cleanup')) {
            $this->client->indices()->delete(['index' => '*']);
        }
    }

    public function grabFromElasticsearch($index = null, $type = null, $queryString = '*')
    {
        $result = $this->client->search(
            [
                'index' => $index,
                'type' => $type,
                'q' => $queryString,
                'size' => 1
            ]
        );

        return !empty($result['hits']['hits'])
            ? $result['hits']['hits'][0]['_source']
            : array();
    }

    public function seeInElasticsearch($index, $type, $fields)
    {
        $query = array_map(function ($value, $key) {
            return ['match' => [$key => $value]];
        }, $fields, array_keys($fields));

        $result = $this->client->search([
            'index' => $index,
            'type' => $type,
            'size' => 1,
            'body' => ['query' => ['bool' => ['filter' => $query]]],
        ]);

        return $this->assertFalse(empty($result['hits']['hits']), 'item exists');
    }

    public function dontSeeInElasticsearch($index, $type, $fields)
    {
        $query = array_map(function ($value, $key) {
            return ['match' => [$key => $value]];
        }, $fields, array_keys($fields));

        $result = $this->client->search([
            'index' => $index,
            'type' => $type,
            'size' => 1,
            'body' => ['query' => ['bool' => ['filter' => $query]]],
        ]);

        return $this->assertTrue(empty($result['hits']['hits']), 'item does not exist');
    }


}