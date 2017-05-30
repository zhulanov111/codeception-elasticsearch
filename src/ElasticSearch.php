<?php
/**
 * @author Jan Wyszynski
 */

namespace Codeception\Module;

use Codeception\Module;
use Codeception\Lib\ModuleContainer;

use Elasticsearch\ClientBuilder;

class Elasticsearch extends Module
{
    public $elasticsearch;

    public function _initialize()
    {
        /*
         * elastic search config
         * hosts - array of ES hosts
         * dic - ES dictionary
         */

        $clientBuilder = ClientBuilder::create();
        $clientBuilder->setHosts($this->_getConfig('hosts'));
        $this->elasticsearch = $clientBuilder->build();      
    }

    /**
     * check if an item exists in a given index
     *
     * @param string $index index name
     * @param string $type item type
     * @param string $id item id
     *
     * @return array
     */
    public function seeItemExistsInElasticsearch($index, $type, $id)
    {
        return $this->elasticsearch->exists(
            [
                'index' => $index,
                'type' => $type,
                'id' => $id
            ]
        );
    }


    /**
     * grab an item from search index
     *
     * @param null $index
     * @param null $type
     * @param string $queryString
     *
     * @return array
     */
    public function grabAnItemFromElasticsearch($index = null, $type = null, $queryString = '*')
    {
        $result = $this->elasticsearch->search(
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
        return $this->assertTrue($this->elasticsearch->exists($params), 'document exists');
    }

    public function dontSeeInElasticsearch($params)
    {
        return $this->assertFalse($this->elasticsearch->exists($params), 'document doesn\'t exist');
    }


}