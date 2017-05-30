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

    public function seeItemExistsInElasticsearch($index, $type, $id)
    {
        return $this->client->exists(
            [
                'index' => $index,
                'type' => $type,
                'id' => $id
            ]
        );
    }

    public function grabAnItemFromElasticsearch($index = null, $type = null, $queryString = '*')
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

    public function seeInElasticsearch($params)
    {
        return $this->assertTrue($this->client->exists($params), 'document exists');
    }

    public function dontSeeInElasticsearch($params)
    {
        return $this->assertFalse($this->client->exists($params), 'document doesn\'t exist');
    }


}